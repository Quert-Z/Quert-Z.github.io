#!/usr/bin/env python3
"""
update-stocks-data.py
Refreshes the website's embedded stock dashboard data. Designed to run in
GitHub Actions on weekday evenings (ET); also runnable locally.

Maintains four JSON files in the repo root:
  stocks-data.json        Valuation tab (prices, core P/E, forward P/E, rev growth)
  stocks-graph.json       Graph Analysis tab (daily closes, SMA50/200, signal)
  stocks-fundamental.json Fundamental tab (quarterly TTM rev/EPS/FCF/EBITA + CAGRs)
  stocks-earnings.json    Earnings + Filings tab (next earnings dates, filings list)

Pipeline per stock:
  prices:      Stooq CSV -> Nasdaq quote API -> Yahoo quote API, per ticker:
               each ticker falls through to the next source on its own when a
               source misses it or serves it stale, so one bad symbol can no
               longer freeze while the rest update (V/ASML/SPY, Nov 5-13 2026)
  core inputs: SEC EDGAR companyfacts (recomputed only when a new 10-Q/10-K/20-F
               appears in the submissions feed; otherwise carried forward)
  forward P/E: Yahoo quote API forwardPE (carried forward when unavailable)
  rev growth:  discrete quarterly revenue Y/Y from companyfacts (carried forward)
  debt/equity: companyfacts balance sheet (carried forward)
  graph:       each weekday's close is appended to the daily history; SMAs and the
               signal are recomputed (BUY when close < 200-day SMA; HOLD when
               close > 50-day SMA, otherwise HOLD; SPY: BUY when close < 50-day SMA)
  fundamental: a new quarter is appended only when EDGAR submissions shows a new
               10-Q/10-K/20-F for that ticker (same trigger as core P/E); TTM values
               are computed from companyfacts discrete quarterly values
               (Q4 = FY annual minus Q1-Q3; MSFT fiscal Jun 30 YE, V fiscal Sep 30 YE)
  earnings:    filings are appended from EDGAR submissions for the 6 covered stocks
               (GOOG, META, AMZN, MSFT, NFLX, UBER); MA/V/ASML filings are
               never added. Earnings dates are carried forward and only replaced by
               a confirmed future date from Yahoo calendarEvents - never invented.

The JSONs from the previous run are read first; any field that cannot be freshly
verified keeps its previous value. The script never writes a null over a good
value and never crashes on a single stock's failure. ASML is carried forward
whole (IFRS filer; annual 20-F). V has no Valuation row, so its fundamental
quarter is refreshed by a dedicated lightweight path (refresh_fundamental_only).
"""

import json
import os
import re
import sys
import time
import datetime
import urllib.request
import urllib.error
import http.cookiejar

UA = {"User-Agent": "QuertZ-Stocks-Dashboard/1.0", "Accept": "application/json"}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "stocks-data.json")
GRAPH_PATH = os.path.join(BASE_DIR, "stocks-graph.json")
FUND_PATH = os.path.join(BASE_DIR, "stocks-fundamental.json")
EARN_PATH = os.path.join(BASE_DIR, "stocks-earnings.json")
GRAPH_HISTORY_CAP = 1500   # ~6 years of daily closes; oldest dropped past this
FILINGS_CAP = 400          # filings list cap (matches the dashboard tab)
FILING_FORMS = ("8-K", "10-Q", "10-K", "4", "13D", "13G", "13D/A", "13G/A")

# NYSE full-day holidays — the Stooq freshness check below must not treat a
# holiday as "Stooq is stale": on a holiday Stooq legitimately still shows the
# previous close, and falling through would stamp the holiday's date on it.
# Refresh this list each January (2026-11-26 and 2026-12-25 are the remaining
# 2026 closures; the 2027 dates are the standard NYSE calendar).
NYSE_HOLIDAYS = {
    "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
    "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}
# tickers whose fundamental quarter is maintained (fiscal calendar per ticker)
FUND_FISCAL = {"GOOG": "dec", "META": "dec", "AMZN": "dec", "MSFT": "msft",
               "NFLX": "dec", "UBER": "dec", "MA": "dec", "V": "sep"}
# filings coverage (the 6 EDGAR stocks) and the one fundamental-only ticker
EARNING_CIK = {"GOOG": "1652044", "META": "1326801", "AMZN": "1018724",
               "MSFT": "789019", "NFLX": "1065280", "UBER": "1543151"}
V_CIK = "1403161"  # Visa Inc. (corrected Nov 5, 2026; 0001404655 is HubSpot)

# ticker, cik (unpadded), currency, short-term-investments tag, fiscal calendar
STOCKS = [
    ("GOOG", "1652044", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("META", "1326801", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("AMZN", "1018724", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("MSFT", "789019",  "USD", "ShortTermInvestments",        "msft"),
    ("NFLX", "1065280", "USD", None,                          "dec"),
    ("UBER", "1543151", "USD", None,                          "dec"),
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


def fetch_price_nasdaq_hist(ticker):
    """Latest close + true trading date from Nasdaq's historical endpoint.
    Fallback for SPY: the quote endpoint above rejects it under both
    assetclass values, but the historical endpoint serves it with
    assetclass=etf (verified 2026-11-14)."""
    frm = (datetime.date.today() - datetime.timedelta(days=14)).isoformat()
    url = (f"https://api.nasdaq.com/api/quote/{ticker}/historical"
           f"?assetclass=etf&fromdate={frm}&limit=100")
    d = http_get_json(url, timeout=20)
    row = d["data"]["tradesTable"]["rows"][0]
    price = float(row["close"].replace(",", "").replace("$", ""))
    m, dd, y = row["date"].split("/")
    return price, f"{y}-{m}-{dd}"


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
    """Per-ticker cascade: Stooq -> Nasdaq -> Yahoo. Returns {ticker: (price, date)}.

    Each ticker is filled independently: a ticker that a source misses — or
    serves stale while the source's other rows are current — falls through to
    the next source on its own. The old all-or-nothing hand-off froze V, ASML
    and SPY at Nov 5, 2026 for 9 days: Stooq's combined response kept passing
    the freshness check on the other tickers' rows while those three stopped
    arriving, and the Nasdaq step returned its partial dict without ever
    trying Yahoo for the missing ones. SPY's quote-API route is dead —
    Nasdaq rejects it under both assetclass values — so it falls back to
    Nasdaq's historical endpoint (assetclass=etf), then Yahoo. And V/ASML
    were missing from the fetch list entirely (see main), so no source was
    ever asked for them.

    Stooq rows carry their own date and are accepted per ticker only at
    Stooq's newest vintage, and only when that vintage is current: if the
    newest Stooq date is older than today — and today is not a NYSE holiday —
    Stooq has not published today's closes yet, so no Stooq row is used
    (Nov 10, 2026: Stooq still served Nov 9 closes at 17:05 EST).
    """
    today = datetime.date.today().isoformat()
    out = {}
    try:
        stooq = fetch_prices_stooq(tickers)
        newest = max(d for _, d in stooq.values())
        if newest >= today or today in NYSE_HOLIDAYS:
            for t in tickers:
                if t in stooq and stooq[t][1] == newest:
                    out[t] = stooq[t]
            log(f"prices: Stooq filled {sorted(out)} (vintage {newest})")
        else:
            log(f"prices: Stooq stale (newest {newest}, today {today})")
    except Exception as e:
        log(f"prices: Stooq failed ({e})")
    for t in tickers:
        if t in out:
            continue
        try:
            out[t] = fetch_price_nasdaq(t)
        except Exception as e:
            # Nasdaq's quote endpoint rejects SPY under both assetclass
            # values; its historical endpoint (assetclass=etf) serves it.
            if t == BENCHMARK:
                try:
                    out[t] = fetch_price_nasdaq_hist(t)
                    continue
                except Exception:
                    pass
            log(f"prices: Nasdaq failed for {t} ({e})")
    missing = [t for t in tickers if t not in out]
    if missing:
        try:
            for r in yahoo_results(missing):
                t = r["symbol"].upper()
                if "regularMarketPrice" in r:
                    out[t] = (float(r["regularMarketPrice"]),
                              datetime.date.today().isoformat())
        except Exception as e:
            log(f"prices: Yahoo failed ({e})")
    still = [t for t in tickers if t not in out]
    if still:
        log(f"prices: NO SOURCE for {still}; carried forward")
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
    if fiscal == "sep":
        # Visa: fiscal year ends Sep 30. Q1 = Oct-Dec, Q2 = Jan-Mar,
        # Q3 = Apr-Jun, Q4 = Jul-Sep.
        if (m, d) == (9, 30):
            fy, fq = y, 4
        elif (m, d) == (12, 31):
            fy, fq = y + 1, 1
        elif (m, d) == (3, 31):
            fy, fq = y, 2
        elif (m, d) == (6, 30):
            fy, fq = y, 3
        else:
            return []
        def se(f, q):
            return {1: (f"{f-1}-10-01", f"{f-1}-12-31"),
                    2: (f"{f}-01-01", f"{f}-03-31"),
                    3: (f"{f}-04-01", f"{f}-06-30"),
                    4: (f"{f}-07-01", f"{f}-09-30")}[q]
        ff, qq = fy, fq
        for _ in range(4):
            s, e = se(ff, qq)
            out.append((s, e, ff, qq))
            qq -= 1
            if qq == 0:
                qq, ff = 4, ff - 1
    elif fiscal == "msft":
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
    elif fiscal == "sep":
        fy = year
        fye = pick_fy(units, f"{fy-1}-10-01", f"{fy}-09-30")
        if fye is None:
            return None
        SE = {1: (f"{fy-1}-10-01", f"{fy-1}-12-31"),
              2: (f"{fy}-01-01", f"{fy}-03-31"),
              3: (f"{fy}-04-01", f"{fy}-06-30")}
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


# ------------------------------------------------- fundamental append ---

def quarter_label(report_date):
    """'Q{q} {year}' from the calendar quarter of the period end."""
    y, m, d = int(report_date[:4]), int(report_date[5:7]), int(report_date[8:10])
    q = {(3, 31): 1, (6, 30): 2, (9, 30): 3, (12, 31): 4}.get((m, d))
    return f"Q{q} {y}" if q else None


def ttm_tag(facts, tag, report_date, fiscal):
    """TTM sum of a discrete quarterly tag (op income, EPS, amortization)."""
    units = units_of(facts, tag)
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


def ttm_tag_multi(facts, tags, report_date, fiscal):
    """TTM sum with per-quarter tag fallback (revenue: the tag a filer uses
    changes over time - e.g. Visa switched 'Revenues' to
    'RevenueFromContractWithCustomerExcludingAssessedTax' in 2019, and
    Mastercard's 2016 quarters are tagged 'SalesRevenueNet'). Each quarter's
    value comes from the first tag that yields it; the Q4 derivation is done
    within one tag, so tags are never mixed inside a single quarter."""
    qdefs = quarter_defs(report_date, fiscal)
    if len(qdefs) != 4:
        return None
    total = 0
    for (s, e, yr, q) in qdefs:
        v = None
        for tag in tags:
            units = units_of(facts, tag)
            if not units:
                continue
            v = discrete_quarterly(units, s, e, yr, q, fiscal)
            if v is not None:
                break
        if v is None:
            return None
        total += v
    return total


def _fy_start(yr, q, fiscal):
    """Fiscal-year start date for the quarter (yr, q) from quarter_defs."""
    if fiscal == "msft":
        return f"{yr-1}-07-01"
    if fiscal == "sep":
        return f"{yr-1}-10-01"
    return f"{yr}-01-01"


def _cal_q(end):
    """(calendar year, calendar quarter) of a period-end date."""
    y, m, d = int(end[:4]), int(end[5:7]), int(end[8:10])
    return y, {(3, 31): 1, (6, 30): 2, (9, 30): 3, (12, 31): 4}[(m, d)]


def ytd_quarters(units, report_date, fiscal):
    """4 discrete quarterly values (oldest first) derived from YTD cash-flow
    tags: fiscal Q1 = YTD Q1; Qn = YTD Qn - YTD Q(n-1); fiscal Q4 = FY - YTD Q3.
    Returns None if any quarter is unavailable."""
    if not units:
        return None
    qdefs = quarter_defs(report_date, fiscal)
    if len(qdefs) != 4:
        return None
    qdefs = list(reversed(qdefs))  # oldest first
    out = []
    for i, (s, e, yr, q) in enumerate(qdefs):
        fystart = _fy_start(yr, q, fiscal)
        cy, cq = _cal_q(e)
        frame = (cy, cq) if fiscal == "dec" else (None, None)
        ytd = pick_q(units, fystart, e, *frame)
        if ytd is None:
            return None
        if q == 1:
            out.append(ytd["val"])
        else:
            # Previous quarter end: for the oldest quarter in the window
            # (a fiscal Q4) it is NOT in qdefs - it is the fiscal Q3, one
            # quarter before the window. Compute it from the end date.
            pe = _prev_q_end(e)
            pcy, pcq = _cal_q(pe)
            pframe = (pcy, pcq) if fiscal == "dec" else (None, None)
            prev = pick_q(units, fystart, pe, *pframe)
            if prev is None:
                return None
            out.append(ytd["val"] - prev["val"])
    return out


def _prev_q_end(end):
    """Quarter-end date of the quarter before the one ending at `end`."""
    y, m, d = int(end[:4]), int(end[5:7]), int(end[8:10])
    if (m, d) == (3, 31):
        return f"{y-1}-12-31"
    if (m, d) == (6, 30):
        return f"{y}-03-31"
    if (m, d) == (9, 30):
        return f"{y}-06-30"
    return f"{y}-09-30"


CAPEX_TAGS = ["PaymentsToAcquirePropertyPlantAndEquipment",
              "PaymentsToAcquireProductiveAssets"]  # Amazon switched to this tag in 2017


OCF_TAGS = ["NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByOperatingActivities"]


def ttm_fcf(facts, report_date, fiscal):
    """TTM free cash flow (OCF - |CapEx|) from YTD cash-flow tags. Each tag
    list is tried in order and the first tag whose YTD picks actually resolve
    for this quarter wins - a tag can have stale entries (Amazon's
    PaymentsToAcquirePropertyPlantAndEquipment stops in 2017; Visa's capex
    is tagged PaymentsToAcquireProductiveAssets) so 'has entries' alone is
    not enough."""
    ocf = None
    for tag in OCF_TAGS:
        ocf = ytd_quarters(units_of(facts, tag), report_date, fiscal)
        if ocf is not None:
            break
    cx = None
    for tag in CAPEX_TAGS:
        cx = ytd_quarters(units_of(facts, tag), report_date, fiscal)
        if cx is not None:
            break
    if ocf is None or cx is None:
        return None
    return sum(o - abs(c) for o, c in zip(ocf, cx))


def ttm_ebita(facts, report_date, fiscal):
    """TTM EBITA = TTM operating income + TTM amortization of intangibles.
    None when amortization is not tagged quarterly (Mastercard and Visa tag it
    annually only, so their EBITA is always null - by design, not an error).
    GOOG and META are also null: their amortization is disclosed in the 10-Q
    notes, not as a quarterly companyfacts tag (GOOG's tag only covers
    2025-2026 10-Qs with no FY annual to derive Q4; META's tag stops in 2025).
    Exception: when a filer has NO amortization tag entries at all (Netflix),
    separately-disclosed intangible amortization is immaterial and EBITA equals
    operating income - the dashboard's own NFLX series is built that way."""
    oi = ttm_tag(facts, "OperatingIncomeLoss", report_date, fiscal)
    if oi is None:
        return None
    am_units = units_of(facts, "AmortizationOfIntangibleAssets")
    if not am_units:
        return oi
    am = ttm_tag(facts, "AmortizationOfIntangibleAssets", report_date, fiscal)
    if am is None:
        return None
    return oi + am


def compute_quarter_point(facts, report_date, fiscal, ticker):
    """TTM point for the quarter ending at report_date. Values in USD millions
    (EPS per share). A component is None when it cannot be derived from EDGAR
    (e.g. V EPS/FCF/EBITA, MA EBITA). Returns None when the quarter label is
    unknown. Note: Mastercard's 10:1 split (Oct 2014) is handled in the seed
    history; this path only ever appends post-split quarters, so no adjustment
    is applied here."""
    label = quarter_label(report_date)
    if not label:
        return None
    rev = ttm_tag_multi(facts, REV_TAGS, report_date, fiscal)
    eps = ttm_tag(facts, "EarningsPerShareDiluted", report_date, fiscal)
    fcf = ttm_fcf(facts, report_date, fiscal)
    ebita = ttm_ebita(facts, report_date, fiscal)
    return {"q": label,
            "rev_m": round(rev / 1e6, 1) if rev is not None else None,
            "eps": round(eps, 2) if eps is not None else None,
            "fcf_m": round(fcf / 1e6, 1) if fcf is not None else None,
            "ebita_m": round(ebita / 1e6, 1) if ebita is not None else None}


def fund_cagrs(points):
    """Revenue/EPS CAGRs (1Y/2Y/5Y/10Y/15Y) from a TTM points series."""
    def cagr(new, old, years):
        if new is None or old is None or old <= 0 or new <= 0:
            return None
        return round(((new / old) ** (1 / years) - 1) * 100, 1)
    out = {}
    for key in ("rev_m", "eps"):
        latest = points[-1].get(key) if points else None
        for n, back in ((1, 4), (2, 8), (5, 20), (10, 40), (15, 60)):
            out[f"{key}_{n}y"] = (cagr(latest, points[-1 - back].get(key), n)
                                  if len(points) > back else None)
    return out


def refresh_fundamental_only(ticker, cik, fiscal, prev_fund):
    """For tickers with no Valuation row (V): fetch submissions; if the newest
    10-Q/10-K is a quarter the fundamental file does not have yet, fetch facts
    and compute the point. Returns a point dict or None."""
    try:
        sub = edgar_submissions(cik)
        latest = latest_10qk(sub)
    except Exception as e:
        log(f"{ticker}: submissions fetch failed ({e}); carrying forward")
        return None
    if not latest:
        return None
    rdate, form, acc = latest
    label = quarter_label(rdate)
    pts = prev_fund.get("stocks", {}).get(ticker, {}).get("points", [])
    if pts and pts[-1]["q"] == label:
        return None
    try:
        facts = edgar_facts(cik)
        pt = compute_quarter_point(facts, rdate, fiscal, ticker)
        if pt:
            log(f"{ticker}: new quarter {label} from {form} {rdate}")
        return pt
    except Exception as e:
        log(f"{ticker}: quarter point failed ({e})")
        return None


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



# ------------------------------------------------- graph / earnings ---

def sma(values, n):
    if len(values) < n:
        return None
    return round(sum(values[-n:]) / n, 2)


def graph_signal(close, s50, s200, spy=False):
    """BUY when close < 200-day SMA; HOLD (red) when close > 50-day SMA;
    otherwise HOLD (gray). SPY: BUY when close < 50-day SMA, HOLD when above."""
    if spy:
        return "BUY" if (s50 is not None and close < s50) else "HOLD"
    if s200 is not None and close < s200:
        return "BUY"
    if s50 is not None and close > s50:
        return "HOLD"
    return "HOLD"


def update_graph(prev, prices):
    """Append each ticker's latest close (when newer than the last history
    date) and recompute SMA50/SMA200 and the signal. Carries forward on any
    failure; never appends a duplicate date."""
    if not prev or not prev.get("stocks"):
        return prev
    for t, entry in prev["stocks"].items():
        if t not in prices:
            log(f"graph {t}: no price this run; carried forward")
            continue
        price, pdate = prices[t]
        if not price or not pdate:
            continue
        hist = entry.get("history", [])
        if hist and pdate <= hist[-1]["d"]:
            continue
        hist.append({"d": pdate, "c": round(float(price), 2)})
        if len(hist) > GRAPH_HISTORY_CAP:
            hist = hist[-GRAPH_HISTORY_CAP:]
        entry["history"] = hist
        closes = [h["c"] for h in hist]
        s50, s200 = sma(closes, 50), sma(closes, 200)
        entry["latest"] = {"date": pdate, "close": round(float(price), 2),
                           "sma50": s50, "sma200": s200,
                           "signal": graph_signal(price, s50, s200, spy=(t == "SPY"))}
        log(f"graph {t}: appended {pdate} close {price}")
    dates = [s["latest"]["date"] for s in prev["stocks"].values() if s.get("latest")]
    if dates:
        prev["as_of"] = max(dates)
    return prev


def update_fundamental(prev, new_points):
    """Append new quarter points (computed by refresh_stock / V path).
    A point whose quarter already exists is skipped. Recomputes CAGRs and
    latest. ASML is never appended (carried forward whole)."""
    if not prev or not prev.get("stocks"):
        return prev
    for ticker, pt in new_points.items():
        if pt is None or ticker not in prev["stocks"]:
            continue
        entry = prev["stocks"][ticker]
        pts = entry.get("points", [])
        if pts and pts[-1]["q"] == pt["q"]:
            continue
        pts.append(pt)
        entry["points"] = pts
        entry["latest"] = dict(pt)
        entry["cagr"] = fund_cagrs(pts)
        log(f"fund {ticker}: appended {pt['q']}")
    def q_end(q):
        m = re.match(r"Q(\d) (\d{4})", q or "")
        if not m:
            return ""
        return f"{m.group(2)}-{['03-31', '06-30', '09-30', '12-31'][int(m.group(1)) - 1]}"
    ends = [q_end(s["latest"]["q"]) for s in prev["stocks"].values() if s.get("latest")]
    if ends:
        prev["as_of"] = max(ends)
    return prev


def sec_url(cik, acc):
    """SEC filing index page URL for an accession number."""
    return (f"https://www.sec.gov/Archives/edgar/data/{int(str(cik).zfill(10))}/"
            f"{acc.replace('-', '')}/{acc}-index.htm")


def yahoo_earnings_date(ticker):
    """Next earnings date (YYYY-MM-DD) from Yahoo calendarEvents, or None.
    Best-effort only: any failure returns None and the stored date stands."""
    global _yahoo_opener
    if _yahoo_opener is None:
        cj = http.cookiejar.CookieJar()
        _yahoo_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        _yahoo_opener.addheaders = [("User-Agent", UA["User-Agent"])]
        _yahoo_opener.open("https://fc.yahoo.com", timeout=20).read()
    crumb = _yahoo_opener.open(
        "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=20).read().decode()
    url = (f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
           f"?modules=calendarEvents&crumb={crumb}")
    d = json.loads(_yahoo_opener.open(url, timeout=25).read())
    res = (d.get("quoteSummary") or {}).get("result") or []
    if not res:
        return None
    ev = res[0].get("calendarEvents") or {}
    dates = (ev.get("earnings") or {}).get("earningsDate") or []
    if not dates:
        return None
    first = dates[0]
    if first.get("fmt"):
        return first["fmt"]
    if first.get("raw"):
        return datetime.datetime.utcfromtimestamp(first["raw"]).date().isoformat()
    return None


def update_earnings(prev):
    """Append new filings from EDGAR submissions (6 covered stocks) and
    best-effort refresh next earnings dates from Yahoo calendarEvents.
    A stored date is only replaced by a confirmed future date; a date that
    has passed with no replacement is marked 'reported' and kept. Dates are
    never invented."""
    if not prev:
        return prev
    seen = {f["accession"] for f in prev.get("filings", [])}
    # Only filings NEWER than the newest date already in the file are appended.
    # The submissions "recent" list reaches back months, and the seed only holds
    # the most recent ~100 filings - without this guard a single run would
    # swallow thousands of old filings up to the 400 cap.
    existing_dates = [f["date"] for f in prev.get("filings", []) if f.get("date")]
    max_date = max(existing_dates) if existing_dates else ""
    new = []
    for ticker, cik in EARNING_CIK.items():
        try:
            sub = edgar_submissions(cik)
        except Exception as e:
            log(f"earnings {ticker}: submissions failed ({e})")
            continue
        rec = sub["filings"]["recent"]
        for i in range(len(rec["accessionNumber"])):
            form = rec["form"][i]
            if form not in FILING_FORMS:
                continue
            acc = rec["accessionNumber"][i]
            if acc in seen or rec["filingDate"][i] <= max_date:
                continue
            desc = (rec.get("primaryDocDescription") or [None] * len(rec["accessionNumber"]))[i] \
                or rec["primaryDocument"][i]
            new.append({"date": rec["filingDate"][i], "ticker": ticker, "form": form,
                        "summary": f"{form} — {desc}",
                        "url": sec_url(cik, acc), "accession": acc})
            seen.add(acc)
    if new:
        filings = sorted(prev.get("filings", []) + new,
                         key=lambda f: f["date"], reverse=True)[:FILINGS_CAP]
        prev["filings"] = filings
        prev["filings_count"] = len(filings)
        log(f"earnings: appended {len(new)} new filings")
    today = datetime.date.today().isoformat()
    for cal in prev.get("calendar", []):
        t = cal["ticker"]
        try:
            d = yahoo_earnings_date(t)
        except Exception as e:
            log(f"earnings {t}: Yahoo calendarEvents failed ({e})")
            d = None
        if d and d > today and d != cal.get("date"):
            cal["date"] = d
            cal["status"] = "estimated"
            cal["source"] = "yahoo"
            log(f"earnings {t}: next date updated to {d} (Yahoo)")
        elif cal.get("date") and cal["date"] <= today and cal.get("status") != "reported":
            cal["status"] = "reported"
    return prev

# ---------------------------------------------------------------- main ---

def refresh_stock(ticker, cik, currency, st_inv_tag, fiscal, prev, prices, yahoo):
    """Returns (row, quarter_point). quarter_point is the new TTM point for the
    Fundamental tab when a new 10-Q/10-K triggered a recompute, else None."""
    out = dict(prev) if prev else {"ticker": ticker}
    out["ticker"] = ticker
    out["currency"] = currency
    point = None

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
            if ticker in FUND_FISCAL:
                try:
                    point = compute_quarter_point(facts, rdate, fiscal, ticker)
                except Exception as e:
                    log(f"{ticker}: quarter point failed ({e})")
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
    return out, point


def load_prev(path):
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except Exception as e:
            log(f"could not read {path} ({e}); starting fresh")
    return {}


def main():
    prev_data = load_prev(JSON_PATH)
    prev_graph = load_prev(GRAPH_PATH)
    prev_fund = load_prev(FUND_PATH)
    prev_earn = load_prev(EARN_PATH)
    prev_by_ticker = {s["ticker"]: s for s in prev_data.get("stocks", [])}

    # V and ASML have no Valuation row (V is fundamental-only, ASML is
    # carried forward whole), but their graph series still need prices —
    # leaving them out froze both charts after the last hand-append
    # (Nov 5, 2026) while the other tickers kept updating.
    tickers = [t for t, *_ in STOCKS] + ["V", "ASML", BENCHMARK]
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
    new_points = {}
    for ticker, cik, currency, st_inv_tag, fiscal in STOCKS:
        try:
            row, point = refresh_stock(ticker, cik, currency, st_inv_tag, fiscal,
                                       prev_by_ticker.get(ticker), prices, yahoo)
            stocks.append(row)
            if point:
                new_points[ticker] = point
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

    # V has no Valuation row; refresh its fundamental quarter directly.
    if prev_fund:
        vpt = refresh_fundamental_only("V", V_CIK, "sep", prev_fund)
        if vpt:
            new_points["V"] = vpt

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

    # --- Graph Analysis: append today's closes, recompute SMAs/signals
    if prev_graph:
        update_graph(prev_graph, prices)
        json.dump(prev_graph, open(GRAPH_PATH, "w"), indent=2)
        log(f"wrote {GRAPH_PATH} as_of={prev_graph.get('as_of')}")
    else:
        log("stocks-graph.json: no previous file; skipped (seed it first)")

    # --- Fundamental: append any new quarters
    if prev_fund:
        update_fundamental(prev_fund, new_points)
        json.dump(prev_fund, open(FUND_PATH, "w"), indent=2)
        log(f"wrote {FUND_PATH} as_of={prev_fund.get('as_of')}")
    else:
        log("stocks-fundamental.json: no previous file; skipped (seed it first)")

    # --- Earnings + Filings: append new filings, refresh dates best-effort
    if prev_earn:
        update_earnings(prev_earn)
        json.dump(prev_earn, open(EARN_PATH, "w"), indent=2)
        log(f"wrote {EARN_PATH} filings={prev_earn.get('filings_count')}")
    else:
        log("stocks-earnings.json: no previous file; skipped (seed it first)")


if __name__ == "__main__":
    main()
