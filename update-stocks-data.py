#!/usr/bin/env python3
"""
update-stocks-data.py
Refreshes stocks-data.json for the website's embedded stock dashboard.
Designed to run in GitHub Actions on weekday afternoons (ET); also runnable locally.

Pipeline per stock:
  prices:      Stooq CSV -> Nasdaq quote API -> Yahoo quote API (first success wins)
  core inputs: SEC EDGAR companyfacts (recomputed only when a new 10-Q/10-K/20-F
               appears in the submissions feed; otherwise carried forward)
  forward P/E: Yahoo quote API forwardPE (carried forward when unavailable)
  rev growth:  discrete quarterly revenue Y/Y from companyfacts (carried forward)
  debt/equity: companyfacts balance sheet (carried forward)

The JSON from the previous run is read first; any field that cannot be freshly
verified keeps its previous value. The script never writes a null over a good
value and never crashes on a single stock's failure. ASML is carried forward
whole (IFRS filer; annual 20-F).
"""

import json
import os
import sys
import time
import datetime
import urllib.request
import urllib.error
import http.cookiejar

UA = {"User-Agent": "QuertZ-Stocks-Dashboard/1.0", "Accept": "application/json"}
JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stocks-data.json")

# ticker, cik (unpadded), currency, short-term-investments tag, fiscal calendar
STOCKS = [
    ("GOOG", "1652044", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("META", "1326801", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("AMZN", "1018724", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("MSFT", "789019",  "USD", "ShortTermInvestments",        "msft"),
    ("NFLX", "1065280", "USD", None,                          "dec"),
    ("UBER", "1543151", "USD", None,                          "dec"),
    ("SPGI", "64040",   "USD", None,                          "dec"),
    ("MA",   "1141391", "USD", None,                          "dec"),
]
BENCHMARK = "SPY"

REV_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
]
DEBT_EXTRA_TAGS = ["DebtCurrent", "ShortTermBorrowings", "CommercialPaper"]


def log(msg):
    print(f"[{datetime.datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_get_json(url, timeout=30):
    return json.loads(http_get(url, timeout))


# ---------------------------------------------------------------- prices ---

def fetch_prices_stooq(tickers):
    syms = ",".join(t.lower() + ".us" for t in tickers)
    url = f"https://stooq.com/q/l/?s={syms}&f=sd2t2ohlcv&h&e=csv"
    txt = http_get(url, timeout=20).decode("utf-8", "replace")
    out = {}
    for line in txt.strip().splitlines()[1:]:
        p = line.split(",")
        if len(p) < 7 or not p[0].strip().lower().endswith(".us"):
            continue
        t = p[0].strip().split(".")[0].upper()
        try:
            out[t] = (float(p[6]), p[1])
        except ValueError:
            continue
    if not out:
        raise RuntimeError("Stooq returned no rows")
    return out


def fetch_price_nasdaq(ticker):
    url = f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=stocks"
    d = http_get_json(url, timeout=20)
    pd = d["data"]["primaryData"]
    price = float(pd["lastSalePrice"].replace(",", "").replace("$", ""))
    return price, datetime.date.today().isoformat()


_yahoo_opener = None


def yahoo_results(symbols):
    """quoteResponse results (regularMarketPrice, forwardPE)."""
    global _yahoo_opener
    if _yahoo_opener is None:
        cj = http.cookiejar.CookieJar()
        _yahoo_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        _yahoo_opener.addheaders = [("User-Agent", UA["User-Agent"])]
        _yahoo_opener.open("https://fc.yahoo.com", timeout=20).read()
    crumb = _yahoo_opener.open(
        "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=20).read().decode()
    url = ("https://query1.finance.yahoo.com/v7/finance/quote?symbols="
           + ",".join(symbols) + f"&crumb={crumb}"
           + "&fields=regularMarketPrice,forwardPE")
    d = json.loads(_yahoo_opener.open(url, timeout=25).read())
    return d["quoteResponse"]["result"]


def fetch_prices(tickers):
    """Cascade: Stooq -> Nasdaq -> Yahoo. Returns {ticker: (price, date)}."""
    try:
        return fetch_prices_stooq(tickers)
    except Exception as e:
        log(f"prices: Stooq failed ({e}); trying Nasdaq")
    out = {}
    for t in tickers:
        try:
            out[t] = fetch_price_nasdaq(t)
        except Exception as e:
            log(f"prices: Nasdaq failed for {t} ({e})")
    if out:
        return out
    log("prices: Nasdaq failed; trying Yahoo")
    res = yahoo_results(tickers)
    for r in res:
        t = r["symbol"].upper()
        if "regularMarketPrice" in r:
            out[t] = (float(r["regularMarketPrice"]),
                      datetime.date.today().isoformat())
    if not out:
        raise RuntimeError("all price sources failed")
    return out


# ---------------------------------------------------------------- EDGAR ---

def edgar_submissions(cik):
    c = str(cik).zfill(10)
    return http_get_json(f"https://data.sec.gov/submissions/CIK{c}.json")


def edgar_facts(cik):
    c = str(cik).zfill(10)
    return http_get_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json")


def latest_10qk(sub):
    """(reportDate, form, accession) of the newest 10-Q/10-K/20-F, or None."""
    rec = sub["filings"]["recent"]
    best = None
    for form, rdate, acc in zip(rec["form"], rec["reportDate"], rec["accessionNumber"]):
        if form in ("10-Q", "10-K", "20-F"):
            if best is None or rdate > best[0]:
                best = (rdate, form, acc)
    return best


def units_of(facts, tag):
    try:
        u = facts["facts"]["us-gaap"][tag]["units"]
    except KeyError:
        return []
    for k in ("USD", "shares", "pure"):
        if k in u:
            return u[k]
    return next(iter(u.values()))


def pick_q(units, start, end, year=None, q=None):
    """One discrete-quarterly entry: exact start/end, 10-Q/10-K, prefer CY frame."""
    cands = [e for e in units
             if e.get("start") == start and e.get("end") == end
             and e.get("form") in ("10-Q", "10-K")]
    if not cands:
        return None
    if year and q:
        fr = f"CY{year}Q{q}"
        frc = [e for e in cands if e.get("frame") == fr]
        if frc:
            cands = frc
    return max(cands, key=lambda e: e.get("filed", ""))


def pick_fy(units, start, end):
    cands = [e for e in units
             if e.get("start") == start and e.get("end") == end
             and e.get("form") in ("10-K", "20-F")]
    if not cands:
        return None
    return max(cands, key=lambda e: e.get("filed", ""))


CAL = {1: ("01-01", "03-31"), 2: ("04-01", "06-30"),
       3: ("07-01", "09-30"), 4: ("10-01", "12-31")}


def quarter_defs(report_date, fiscal):
    """[(start, end, year_label, q)] for the 4 quarters ending at report_date,
    newest first. Handles Dec calendar and MSFT fiscal (Jun 30 YE)."""
    y, m, d = int(report_date[:4]), int(report_date[5:7]), int(report_date[8:10])
    out = []
    if fiscal == "msft":
        if (m, d) == (9, 30):
            fy, fq = y + 1, 1
        elif (m, d) == (12, 31):
            fy, fq = y + 1, 2
        elif (m, d) == (3, 31):
            fy, fq = y, 3
        elif (m, d) == (6, 30):
            fy, fq = y, 4
        else:
            return []
        def se(f, q):
            return {1: (f"{f-1}-07-01", f"{f-1}-09-30"),
                    2: (f"{f-1}-10-01", f"{f-1}-12-31"),
                    3: (f"{f}-01-01", f"{f}-03-31"),
                    4: (f"{f}-04-01", f"{f}-06-30")}[q]
        ff, qq = fy, fq
        for _ in range(4):
            s, e = se(ff, qq)
            out.append((s, e, ff, qq))
            qq -= 1
            if qq == 0:
                qq, ff = 4, ff - 1
    else:
        q = {(3, 31): 1, (6, 30): 2, (9, 30): 3, (12, 31): 4}.get((m, d))
        if not q:
            return []
        yy, qq = y, q
        for _ in range(4):
            s, e = CAL[qq]
            out.append((f"{yy}-{s}", f"{yy}-{e}", yy, qq))
            qq -= 1
            if qq == 0:
                qq, yy = 4, yy - 1
    return out


def discrete_quarterly(units, start, end, year, q, fiscal):
    """Discrete quarterly value; derives Q4/FQ4 as FY - Q1-Q2-Q3 when needed."""
    e = pick_q(units, start, end, year if fiscal == "dec" else None,
               q if fiscal == "dec" else None)
    if e is not None:
        return e["val"]
    if q != 4:
        return None
    if fiscal == "msft":
        fy = year
        fye = pick_fy(units, f"{fy-1}-07-01", f"{fy}-06-30")
        if fye is None:
            return None
        SE = {1: (f"{fy-1}-07-01", f"{fy-1}-09-30"),
              2: (f"{fy-1}-10-01", f"{fy-1}-12-31"),
              3: (f"{fy}-01-01", f"{fy}-03-31")}
    else:
        fye = pick_fy(units, f"{year}-01-01", f"{year}-12-31")
        if fye is None:
            return None
        SE = {qq: (f"{year}-{CAL[qq][0]}", f"{year}-{CAL[qq][1]}") for qq in (1, 2, 3)}
    parts = []
    for qq in (1, 2, 3):
        s, e2 = SE[qq]
        v = pick_q(units, s, e2)
        if v is None:
            return None
        parts.append(v["val"])
    return fye["val"] - sum(parts)


def ttm_op_income(facts, report_date, fiscal):
    units = units_of(facts, "OperatingIncomeLoss")
    if not units:
        return None
    qdefs = quarter_defs(report_date, fiscal)
    if len(qdefs) != 4:
        return None
    total = 0
    for (s, e, yr, q) in qdefs:
        v = discrete_quarterly(units, s, e, yr, q, fiscal)
        if v is None:
            return None
        total += v
    return total


def latest_bs(units):
    cands = [e for e in units if e.get("form") in ("10-Q", "10-K", "20-F")]
    if not cands:
        return None
    return max(cands, key=lambda e: (e.get("end", ""), e.get("filed", "")))


def balance_inputs(facts, st_inv_tag):
    """(net_debt, total_debt, equity) in filing currency, Nones when unavailable."""
    def val(tag):
        e = latest_bs(units_of(facts, tag))
        return e["val"] if e else 0

    cash = val("CashAndCashEquivalentsAtCarryingValue")
    st_inv = val(st_inv_tag) if st_inv_tag else 0
    if not st_inv_tag:
        for t in ("MarketableSecuritiesCurrent", "ShortTermInvestments"):
            v = val(t)
            if v:
                st_inv = v
                break
    debt = val("LongTermDebtNoncurrent") + val("LongTermDebtCurrent")
    for t in DEBT_EXTRA_TAGS:
        v = val(t)
        if v and debt and v > 0.05 * debt:  # only when material
            debt += v
    eq_e = latest_bs(units_of(facts, "StockholdersEquity"))
    equity = eq_e["val"] if eq_e else None
    if not debt and not cash:
        return None, debt or None, equity
    return debt - cash - st_inv, debt, equity


def shares_out(facts):
    # Weighted-average basic shares for the latest quarter (DEI point-in-time
    # shares are often absent from companyfacts).
    units = units_of(facts, "WeightedAverageNumberOfSharesOutstandingBasic")
    e = latest_bs(units)
    return e["val"] if e else None


def quarterly_revenue_growth(facts, report_date, fiscal):
    """Discrete quarterly revenue Y/Y %. None when unavailable."""
    units = None
    for t in REV_TAGS:
        u = units_of(facts, t)
        if u:
            units = u
            break
    if not units:
        return None
    qdefs = quarter_defs(report_date, fiscal)
    if len(qdefs) != 4:
        return None
    (s0, e0, y0, q0) = qdefs[0]
    v0 = discrete_quarterly(units, s0, e0, y0, q0, fiscal)
    # same quarter, prior year
    if fiscal == "msft":
        fy, fq = y0, q0
        SE = {1: (f"{fy-2}-07-01", f"{fy-2}-09-30"),
              2: (f"{fy-2}-10-01", f"{fy-2}-12-31"),
              3: (f"{fy-1}-01-01", f"{fy-1}-03-31"),
              4: (f"{fy-1}-04-01", f"{fy-1}-06-30")}
        sa, ea = SE[fq]
        va = discrete_quarterly(units, sa, ea, fy - 1, fq, fiscal)
    else:
        s, e = CAL[q0]
        va = discrete_quarterly(units, f"{y0-1}-{s}", f"{y0-1}-{e}", y0 - 1, q0, fiscal)
    if v0 is None or va is None or va == 0:
        return None
    return round((v0 - va) / abs(va) * 100, 1)


# ---------------------------------------------------------------- main ---

def refresh_stock(ticker, cik, currency, st_inv_tag, fiscal, prev, prices, yahoo):
    out = dict(prev) if prev else {"ticker": ticker}
    out["ticker"] = ticker
    out["currency"] = currency

    if ticker in prices:
        price, pdate = prices[ticker]
        out["price"] = round(price, 2)
        out["price_date"] = pdate

    try:
        sub = edgar_submissions(cik)
        latest = latest_10qk(sub)
    except Exception as e:
        log(f"{ticker}: submissions fetch failed ({e}); carrying forward")
        latest = None

    needs_recompute = (not prev) or (prev.get("ttm_op_income") is None) or \
        (bool(latest) and latest[2] != prev.get("last_acc"))
    if latest and needs_recompute:
        rdate, form, acc = latest
        try:
            facts = edgar_facts(cik)
            oi = ttm_op_income(facts, rdate, fiscal)
            net_debt, debt, equity = balance_inputs(facts, st_inv_tag)
            so = shares_out(facts)
            rg = quarterly_revenue_growth(facts, rdate, fiscal)
            if oi is not None:
                out["ttm_op_income"] = oi
                out["vintage"] = rdate
            if net_debt is not None:
                out["net_debt"] = net_debt
            if so:
                out["shares_out"] = so
            if rg is not None:
                out["rev_growth_yoy"] = rg
            if debt and equity:
                out["debt_equity"] = round(debt / equity * 100, 1)
            out["last_acc"] = acc
            log(f"{ticker}: recomputed from {form} {rdate}")
        except Exception as e:
            log(f"{ticker}: EDGAR recompute failed ({e}); carrying forward")

    try:
        fpe = yahoo.get(ticker, {}).get("forwardPE")
        if fpe:
            out["forward_pe"] = round(float(fpe), 2)
    except Exception:
        pass

    try:
        if out.get("price") and out.get("shares_out") and out.get("ttm_op_income"):
            mc = out["price"] * out["shares_out"]
            ev = mc + (out.get("net_debt") or 0)
            out["market_cap_b"] = round(mc / 1e9, 2)
            out["core_pe"] = round(ev / out["ttm_op_income"], 1)
            if out.get("forward_pe"):
                out["avg_pe"] = round((out["core_pe"] + out["forward_pe"]) / 2, 1)
    except Exception as e:
        log(f"{ticker}: metric recompute failed ({e})")
    return out


def main():
    prev_data = {}
    if os.path.exists(JSON_PATH):
        try:
            prev_data = json.load(open(JSON_PATH))
        except Exception as e:
            log(f"could not read existing JSON ({e}); starting fresh")
    prev_by_ticker = {s["ticker"]: s for s in prev_data.get("stocks", [])}

    tickers = [t for t, *_ in STOCKS] + [BENCHMARK]
    try:
        prices = fetch_prices(tickers)
    except Exception as e:
        # Don't crash the whole run on a price outage: carry prices forward.
        # as_of below is derived from price dates, so it stays honest.
        log(f"WARNING: no prices this run ({e}); carrying forward")
        prices = {}

    yahoo = {}
    try:
        for r in yahoo_results(tickers):
            yahoo[r["symbol"].upper()] = r
    except Exception as e:
        log(f"Yahoo failed ({e}); forward P/E carried forward")

    stocks = []
    for ticker, cik, currency, st_inv_tag, fiscal in STOCKS:
        try:
            stocks.append(refresh_stock(ticker, cik, currency, st_inv_tag, fiscal,
                                        prev_by_ticker.get(ticker), prices, yahoo))
        except Exception as e:
            log(f"{ticker}: unexpected failure ({e}); carrying forward")
            if ticker in prev_by_ticker:
                stocks.append(prev_by_ticker[ticker])
        time.sleep(1)  # be polite to EDGAR

    # ASML carried forward whole (IFRS filer; annual 20-F)
    if "ASML" in prev_by_ticker:
        row = dict(prev_by_ticker["ASML"])
        row["ticker"] = "ASML"
        stocks.append(row)
        log("ASML: carried forward (IFRS filer)")
    else:
        log("ASML: no previous data; omitted")

    spy_price, spy_date = prices.get(BENCHMARK, (None, None))
    dates = [s.get("price_date", "") for s in stocks if s.get("price_date")]
    if spy_date:
        dates.append(spy_date)
    as_of = max(dates) if dates else datetime.date.today().isoformat()

    data = {
        "as_of": as_of,
        "stocks": stocks,
        "benchmark": {"ticker": BENCHMARK,
                      "price": round(spy_price, 2) if spy_price else None},
    }
    json.dump(data, open(JSON_PATH, "w"), indent=2)
    log(f"wrote {JSON_PATH} as_of={as_of}")
    for s in stocks:
        log(f"  {s['ticker']}: price={s.get('price')} core_pe={s.get('core_pe')} "
            f"fwd={s.get('forward_pe')} rev_yoy={s.get('rev_growth_yoy')}")


if __name__ == "__main__":
    main()
