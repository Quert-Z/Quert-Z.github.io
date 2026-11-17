#!/usr/bin/env python3
"""
update-stocks-data.py
Refreshes the website's embedded stock dashboard data. Designed to run in
GitHub Actions on weekday evenings (ET); also runnable locally.

Maintains five JSON files in the repo root:
  stocks-data.json        Valuation tab (prices, core P/E, forward P/E, rev growth)
  stocks-graph.json       Graph Analysis tab (daily closes, SMA50/200, signal)
  stocks-fundamental.json Fundamental tab (quarterly TTM rev/EPS/FCF/EBITA + CAGRs)
  stocks-earnings.json    Earnings + Filings tab (next earnings dates, filings list)
  stocks-sim10.json       Simulation tab 10Y view (weekly dividend-adjusted
                          closes since 2015; rebuilt from Yahoo daily
                          adjcloses collapsed to weekly, written only when
                          changed; NOTE: the workflow's git add line must
                          list this file or the Action will not commit it)

Pipeline per stock:
  prices:      dated official closes only, fetched as a multi-session
               window per ticker: Nasdaq historical -> stockanalysis.com
               history -> Yahoo chart (daily closes). The Valuation price
               is the window's newest bar and the graph merges the whole
               window, so a missed session backfills itself on the next
               run and the two files can never disagree. The old quote-API
               leg was removed 2026-11-17: it stamped lastSalePrice (an
               after-hours print at the 19:05 ET run time) with the UTC
               run date, which mislabelled sessions, wrote Saturday bars,
               and — with SPY's only leg the lagging ETF historical feed
               and an unchecked stale-row fallback — froze SPY at Nov 13.
               Only bars dated on or before the last completed session
               (America/New_York) are ever used.
  core inputs: SEC EDGAR companyfacts (recomputed only when a new 10-Q/10-K/20-F
               appears in the submissions feed; otherwise carried forward)
  forward P/E: stockanalysis.com statistics page (same basis as the private
               dashboard), falling back to Yahoo quote API forwardPE, then
               carried forward. The QQQ forward P/E in the graph notes is
               refreshed the same way each run. ASML's row is still carried
               forward whole for its EUR core P/E inputs, but its forward P/E,
               average P/E and revenue growth (TTM, from the Fundamental
               series) are refreshed each run, and so is its debt/equity —
               from ASML's own EDGAR companyfacts (total debt over equity,
               same balance-sheet date; ~22% on FY2025, low because most of
               ASML's liabilities are customer prepayments, not borrowings).
  rev growth:  discrete quarterly revenue Y/Y from companyfacts (carried forward)
  debt/equity: companyfacts balance sheet (carried forward)
  graph:       the fetched window is merged into the daily history each
               run: missing sessions are inserted, wrong closes corrected,
               and bars dated after the last completed session or on a
               non-session day inside the window are dropped; SMAs and the
               signal are recomputed from the series tail (BUY when close
               < 200-day SMA; HOLD when close > 50-day SMA, otherwise HOLD;
               SPY: BUY when close < 50-day SMA).
  fundamental: a new quarter is appended only when EDGAR submissions shows a new
               10-Q/10-K/20-F for that ticker (same trigger as core P/E); TTM values
               are computed from companyfacts discrete quarterly values
               (Q4 = FY annual minus Q1-Q3; MSFT fiscal Jun 30 YE, V fiscal Sep 30 YE)
  earnings:    filings are appended from EDGAR submissions for the 6 covered stocks
               (GOOG, META, AMZN, MSFT, NFLX); MA/ASML/ISRG/SPGI filings are
               never added. Earnings dates are carried forward and only replaced by
               a confirmed future date from Yahoo calendarEvents - never invented.

The JSONs from the previous run are read first; any field that cannot be freshly
verified keeps its previous value. The script never writes a null over a good
value and never crashes on a single stock's failure. ASML's core inputs are
carried forward (IFRS filer; annual 20-F), but its price, market cap and core
P/E are recomputed from the fresh close every run (ASML_SHARES_OUT /
ASML_NET_DEBT_USD below), so its Valuation row can no longer freeze.
"""

import concurrent.futures
import json
import os
import re
import sys
import time
import datetime
import urllib.request
import urllib.error
import http.cookiejar
import zoneinfo

UA = {"User-Agent": "QuertZ-Stocks-Dashboard/1.0", "Accept": "application/json"}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "stocks-data.json")
GRAPH_PATH = os.path.join(BASE_DIR, "stocks-graph.json")
FUND_PATH = os.path.join(BASE_DIR, "stocks-fundamental.json")
EARN_PATH = os.path.join(BASE_DIR, "stocks-earnings.json")
SIM10_PATH = os.path.join(BASE_DIR, "stocks-sim10.json")
SIM10_TICKERS = ["SPY", "MSFT", "GOOG", "AMZN", "META", "MA", "NFLX", "ASML", "ISRG", "SPGI"]
SIM10_START = datetime.datetime(2015, 5, 25, tzinfo=datetime.timezone.utc)  # Monday buffer before the series start (2015-06-01)
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
               "NFLX": "dec", "MA": "dec", "ISRG": "dec", "SPGI": "dec"}
# filings coverage (the 5 EDGAR stocks whose filings feed the Earnings tab)
EARNING_CIK = {"GOOG": "1652044", "META": "1326801", "AMZN": "1018724",
               "MSFT": "789019", "NFLX": "1065280"}

# ticker, cik (unpadded), currency, short-term-investments tag, fiscal calendar
STOCKS = [
    ("GOOG", "1652044", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("META", "1326801", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("AMZN", "1018724", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("MSFT", "789019",  "USD", "ShortTermInvestments",        "msft"),
    ("NFLX", "1065280", "USD", None,                          "dec"),
    ("MA",   "1141391", "USD", None,                          "dec"),
    ("ISRG", "1035267", "USD", "MarketableSecuritiesCurrent", "dec"),
    ("SPGI", "64040",   "USD", None,                          "dec"),
]
BENCHMARK = "SPY"

# ASML's hand-built USD enterprise-value inputs (IFRS filer; refreshed by
# hand on each 20-F, like its other carried inputs). shares_out is the
# count implied by the row's market_cap_b at its Nov 2026 prices;
# net_debt is the USD net debt consistent with the row's core P/E at
# those prices (EV ~$571B at the 2026-11-13 close of $1,495.16, TTM
# operating income $17,098,148,000). They exist so ASML's Valuation
# price / market cap / core P/E recompute from the fresh close every
# run instead of freezing at the last hand edit (the Nov 13 freeze).
ASML_SHARES_OUT = 385_940_000
ASML_NET_DEBT_USD = -6_340_000_000

REV_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
]
DEBT_EXTRA_TAGS = ["DebtCurrent", "ShortTermBorrowings", "CommercialPaper"]


def log(msg):
    print(f"[{datetime.datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)


def run_parallel(fn, items, workers=5):
    """Map fn over items concurrently, preserving order. The updater is
    I/O-bound — dozens of HTTP calls that used to run one after another —
    so pooling them is the single biggest speed-up available (a nightly
    run drops from ~80s to ~20s). Callers keep their own per-item error
    handling; EDGAR asks for at most 10 requests/second and a pool of 5
    stays well under that."""
    items = list(items)
    if len(items) <= 1:
        return [fn(x) for x in items]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fn, items))


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_get_json(url, timeout=30):
    return json.loads(http_get(url, timeout))


# ---------------------------------------------------------------- prices ---

NASDAQ_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0 Safari/537.36",
             "Accept": "application/json"}
ET = zoneinfo.ZoneInfo("America/New_York")
HISTORY_DAYS = 45          # window fetched per ticker each run (~31 sessions)
PRICE_WINDOWS = {}         # ticker -> {date: close} from the latest fetch


def et_today():
    return datetime.datetime.now(ET).date()


def is_trading_day(d):
    return d.weekday() < 5 and d.isoformat() not in NYSE_HOLIDAYS


def last_completed_session():
    """Newest US session whose official close is final, in ET terms.
    Before 16:05 ET on a trading day that is the previous trading day —
    a live or after-hours print is never a close."""
    now = datetime.datetime.now(ET)
    d = now.date()
    if is_trading_day(d) and now.time() >= datetime.time(16, 5):
        return d
    d -= datetime.timedelta(days=1)
    while not is_trading_day(d):
        d -= datetime.timedelta(days=1)
    return d


def fetch_history_nasdaq(ticker, assetclass):
    """{date: close} for the recent window from Nasdaq's historical
    endpoint — official closes under their true trading dates."""
    frm = (et_today() - datetime.timedelta(days=HISTORY_DAYS)).isoformat()
    url = (f"https://api.nasdaq.com/api/quote/{ticker}/historical"
           f"?assetclass={assetclass}&fromdate={frm}&limit=250")
    req = urllib.request.Request(url, headers=NASDAQ_UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        d = json.loads(r.read())
    out = {}
    for row in d["data"]["tradesTable"]["rows"]:
        m, dd, y = row["date"].split("/")
        out[f"{y}-{m}-{dd}"] = float(row["close"].replace(",", "").replace("$", ""))
    return out


def fetch_history_sa(ticker):
    """{date: close} from stockanalysis.com's history API — the fallback
    the SMA-cross job already trusts for SPY. It includes the
    in-progress session during market hours; callers cut the window at
    the last completed session."""
    url = (f"https://stockanalysis.com/api/symbol/s/{ticker}/history"
           f"?range=3M&period=Daily")
    req = urllib.request.Request(url, headers=SA_UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        d = json.loads(r.read())
    return {row["t"]: float(row["c"]) for row in d["data"]}


def fetch_history_yahoo(ticker):
    """{date: close} from Yahoo's chart API (raw daily closes, true
    dates). Last of the dated sources."""
    p2 = int(time.time()) + 86400
    p1 = p2 - (HISTORY_DAYS + 10) * 86400
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?period1={p1}&period2={p2}&interval=1d")
    req = urllib.request.Request(url, headers=NASDAQ_UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.loads(r.read())
    res = d["chart"]["result"][0]
    closes = res["indicators"]["quote"][0]["close"]
    out = {}
    for ts, c in zip(res["timestamp"], closes):
        if c is not None:
            day = datetime.datetime.fromtimestamp(
                ts, datetime.timezone.utc).date().isoformat()
            out[day] = float(c)
    return out


def fetch_histories(tickers):
    """Per ticker, the first dated-close source that answers wins:
    Nasdaq historical -> stockanalysis.com -> Yahoo chart. Returns
    {ticker: {date: close}}; a ticker no source serves is absent and
    carries forward. Windows are cut at the last completed session, so
    an in-progress or future bar can never enter the data."""
    cutoff = last_completed_session().isoformat()

    def one(t):
        ac = "etf" if t == BENCHMARK else "stocks"
        for name, fn in (("Nasdaq", lambda: fetch_history_nasdaq(t, ac)),
                         ("stockanalysis", lambda: fetch_history_sa(t)),
                         ("Yahoo", lambda: fetch_history_yahoo(t))):
            try:
                w = {d: c for d, c in fn().items() if d <= cutoff}
                if w:
                    log(f"history {t}: {name}, {len(w)} bars through {max(w)}")
                    return t, w
                log(f"history {t}: {name} returned no completed bars")
            except Exception as e:
                log(f"history {t}: {name} failed ({e})")
        return t, None

    out = {}
    for t, w in run_parallel(one, tickers, workers=6):
        if w:
            out[t] = w
    return out


def fetch_prices(tickers):
    """{ticker: (price, date)} — each ticker's newest completed close,
    taken from the same dated window the graph merges (PRICE_WINDOWS),
    so the Valuation price and the graph's last bar always agree.
    Replaces the old single-point cascade (Nasdaq quote / Yahoo quote /
    Stooq snapshot), whose dateless last-trade prints were stamped with
    the UTC run date and froze or mislabelled sessions."""
    global PRICE_WINDOWS
    PRICE_WINDOWS = fetch_histories(tickers)
    out = {}
    for t, w in PRICE_WINDOWS.items():
        d = max(w)
        out[t] = (round(w[d], 2), d)
    missing = [t for t in tickers if t not in out]
    if missing:
        log(f"prices: NO DATED SOURCE for {missing}; carried forward")
    if not out:
        raise RuntimeError("all price sources failed")
    return out


# ---------------------------------------------------------- forward P/E ---

SA_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0 Safari/537.36",
         "Accept": "text/html"}
_sa_fpe_cache = {}


def _sa_fetch(url):
    req = urllib.request.Request(url, headers=SA_UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", "replace")


def sa_forward_pe(ticker):
    """Forward P/E from stockanalysis.com's statistics page, which embeds
    the value as id:"peForward",title:"Forward PE",value:"19.48". This is
    the same source the private dashboard is audited against. None on any
    failure (the caller falls back to Yahoo, then carries forward)."""
    if ticker in _sa_fpe_cache:
        return _sa_fpe_cache[ticker]
    val = None
    try:
        html = _sa_fetch(f"https://stockanalysis.com/stocks/{ticker.lower()}/statistics/")
        m = re.search(r'id:"peForward",title:"Forward PE",value:"([0-9.]+)"', html)
        if m:
            val = float(m.group(1))
        else:
            log(f"fwd P/E: stockanalysis pattern not found for {ticker}")
    except Exception as e:
        log(f"fwd P/E: stockanalysis failed for {ticker} ({e})")
    _sa_fpe_cache[ticker] = val
    return val


def sa_qqq_forward_pe():
    """QQQ forward P/E from stockanalysis.com's ETF page (embedded value
    when present, else the stats-table cell). None on failure."""
    try:
        html = _sa_fetch("https://stockanalysis.com/etf/qqq/")
        m = re.search(r'id:"peForward",title:"Forward PE",value:"([0-9.]+)"', html) or \
            re.search(r'Forward PE</td>\s*<td[^>]*>([0-9.]+)', html)
        if m:
            return float(m.group(1))
    except Exception as e:
        log(f"QQQ fwd P/E: stockanalysis failed ({e})")
    return None


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


def update_graph(prev, prices, windows=None):
    """Merge each ticker's dated close window into its history and
    recompute SMA50/SMA200 and the signal from the series tail.

    The merge inserts sessions a previous run missed, corrects closes a
    weaker source got wrong, and drops bars that cannot be real
    sessions (dated after the last completed session, or on a
    weekend/holiday inside the fetched window) — e.g. the
    run-date-labelled quote prints the old cascade wrote (the Saturday
    2026-11-14 bars, the 2026-11-17 after-hours prints). With no
    window, the single (price, date) from prices merges the same way.
    A ticker with neither carries forward untouched."""
    if not prev or not prev.get("stocks"):
        return prev
    cutoff = last_completed_session().isoformat()
    for t, entry in prev["stocks"].items():
        w = dict((windows or {}).get(t) or {})
        if not w and t in prices and prices[t][0] and prices[t][1]:
            w = {prices[t][1]: prices[t][0]}
        if not w:
            if entry.get("history"):
                log(f"graph {t}: no window this run; carried forward")
            continue
        merged = {h["d"]: float(h["c"]) for h in entry.get("history", [])}
        wmin = min(w)
        for d in list(merged):
            if d > cutoff or (d >= wmin and not is_trading_day(
                    datetime.date.fromisoformat(d))):
                del merged[d]
                log(f"graph {t}: dropped non-session bar {d}")
        for d, c in w.items():
            if d > cutoff or not is_trading_day(datetime.date.fromisoformat(d)):
                continue
            c = round(float(c), 2)
            if merged.get(d) != c:
                log(f"graph {t}: {'corrected' if d in merged else 'backfilled'} "
                    f"{d} close {c}")
                merged[d] = c
        hist = [{"d": d, "c": merged[d]} for d in sorted(merged)]
        if len(hist) > GRAPH_HISTORY_CAP:
            hist = hist[-GRAPH_HISTORY_CAP:]
        entry["history"] = hist
        if hist:
            closes = [h["c"] for h in hist]
            s50, s200 = sma(closes, 50), sma(closes, 200)
            last = hist[-1]
            entry["latest"] = {"date": last["d"], "close": last["c"],
                               "sma50": s50, "sma200": s200,
                               "signal": graph_signal(last["c"], s50, s200,
                                                      spy=(t == "SPY"))}
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

def asml_debt_equity():
    """ASML debt/equity % from its EDGAR companyfacts (CIK 937966): total
    debt (LongTermDebtNoncurrent + LongTermDebtCurrent) over
    StockholdersEquity, both taken from the SAME balance-sheet date — the
    latest 20-F or 6-K that carries both, in one consistent unit (EUR).
    None on any failure (the row then keeps its previous value)."""
    try:
        facts = edgar_facts("937966")
    except Exception as e:
        log(f"ASML D/E: companyfacts failed ({e})")
        return None

    def series(tag):
        units = (facts.get("facts", {}).get("us-gaap", {}).get(tag, {}) or {}).get("units", {})
        if not units:
            return {}, None
        unit = "EUR" if "EUR" in units else next(iter(units))
        out = {}
        for e in units[unit]:
            if e.get("form") in ("20-F", "6-K") and e.get("end"):
                out[e["end"]] = e["val"]
        return out, unit

    ltd_nc, u1 = series("LongTermDebtNoncurrent")
    ltd_c, u2 = series("LongTermDebtCurrent")
    eq, u3 = series("StockholdersEquity")
    if len({u for u in (u1, u2, u3) if u}) != 1:
        log("ASML D/E: inconsistent units across tags; skipped")
        return None
    # Require BOTH debt tags at the chosen date: ASML's 6-Ks tag the
    # noncurrent line but not the current portion, so a union would
    # silently understate debt on interim dates.
    for end in sorted(set(eq) & set(ltd_nc) & set(ltd_c), reverse=True):
        debt = (ltd_nc.get(end) or 0) + (ltd_c.get(end) or 0)
        if debt > 0 and eq[end] > 0:
            log(f"ASML D/E: {debt / eq[end] * 100:.1f}% (balance sheet {end})")
            return round(debt / eq[end] * 100, 1)
    log("ASML D/E: no balance-sheet date with both debt and equity")
    return None


# ------------------------------------------------------------- SIM10 ---

def _yahoo_chart(ticker, p1, p2, interval):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?period1={p1}&period2={p2}&interval={interval}&events=div,split")
    hdr = dict(SA_UA)
    hdr["Accept"] = "application/json"  # Yahoo's chart API 406s without it
    req = urllib.request.Request(url, headers=hdr)
    with urllib.request.urlopen(req, timeout=40) as r:
        d = json.loads(r.read())
    res = d["chart"]["result"][0]
    adj = res["indicators"]["adjclose"][0]["adjclose"]
    out = []
    for t, a in zip(res["timestamp"], adj):
        if a is not None:
            day = datetime.datetime.fromtimestamp(t, datetime.timezone.utc).date().isoformat()
            out.append((day, round(float(a), 3)))
    return out


def _week_key(iso):
    """Monday of the week containing iso (same rule as the page's simWeekKey)."""
    d = datetime.date.fromisoformat(iso)
    return (d - datetime.timedelta(days=d.weekday())).isoformat()


def build_sim10(prices):
    """Weekly dividend-adjusted closes since 2015 for the Simulation 10Y
    view, fetched as Yahoo weekly bars (interval=1wk). The bars carry the
    same values as collapsing the daily series to each week's last
    trading day — verified equal within 0.0003% over 600 weeks — at a
    fifth of the download (69 KB vs 317 KB per ticker). The final point
    is the latest trading day itself: Yahoo's adjclose for the newest day
    equals the raw close, so the endpoint is taken from the prices
    cascade whenever it falls in or after the last bar's week.
    All-or-nothing: returns None when any ticker fails or comes back
    thin, so a bad night never shrinks the file."""
    p1 = int(SIM10_START.timestamp())
    p2 = int(time.time()) + 86400

    def one(t):
        try:
            bars = _yahoo_chart(t, p1, p2, "1wk")
        except Exception as e:
            log(f"sim10: Yahoo failed for {t} ({e}); keeping previous file")
            return t, None
        pts = dict(bars)
        pv = prices.get(t)
        if pv and pts:
            pprice, pdate = pv
            last = max(pts)
            if _week_key(pdate) == _week_key(last):
                if pdate != last:
                    del pts[last]
                pts[pdate] = round(float(pprice), 3)
            elif pdate > last:
                pts[pdate] = round(float(pprice), 3)
        pts = sorted(pts.items())
        if len(pts) < 500:
            log(f"sim10: {t} returned only {len(pts)} weekly points; keeping previous file")
            return t, None
        log(f"sim10 {t}: {len(pts)} weekly points, {pts[0][0]} .. {pts[-1][0]}")
        return t, dict(pts)

    weekly = {}
    for t, pts in run_parallel(one, SIM10_TICKERS, workers=6):
        if pts is None:
            return None
        weekly[t] = pts
    dates = sorted({d for pts in weekly.values() for d in pts})
    return {"dates": dates,
            "series": {t: [weekly[t].get(d) for d in dates] for t in SIM10_TICKERS}}


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

    fpe = sa_forward_pe(ticker)
    if not fpe:
        try:
            fpe = yahoo.get(ticker, {}).get("forwardPE")
        except Exception:
            fpe = None
    if fpe:
        out["forward_pe"] = round(float(fpe), 2)

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

    # ASML has no Valuation row (carried forward whole), but its graph
    # series still needs prices - leaving it out froze its chart after
    # the last hand-append (Nov 5, 2026) while the other tickers kept
    # updating.
    tickers = [t for t, *_ in STOCKS] + ["ASML", BENCHMARK]
    try:
        prices = fetch_prices(tickers)
    except Exception as e:
        # Don't crash the whole run on a price outage: carry prices forward.
        # as_of below is derived from price dates, so it stays honest.
        log(f"WARNING: no prices this run ({e}); carrying forward")
        prices = {}

    yahoo = {}
    try:
        for r in yahoo_results(tickers + ["QQQ"]):
            yahoo[r["symbol"].upper()] = r
    except Exception as e:
        log(f"Yahoo failed ({e}); forward P/E carried forward")

    stocks = []
    new_points = {}

    def _refresh(meta):
        ticker, cik, currency, st_inv_tag, fiscal = meta
        try:
            return refresh_stock(ticker, cik, currency, st_inv_tag, fiscal,
                                 prev_by_ticker.get(ticker), prices, yahoo), None
        except Exception as e:
            return None, e

    for meta, (res, err) in zip(STOCKS, run_parallel(_refresh, STOCKS, workers=5)):
        ticker = meta[0]
        if err is not None:
            log(f"{ticker}: unexpected failure ({err}); carrying forward")
            if ticker in prev_by_ticker:
                stocks.append(prev_by_ticker[ticker])
            continue
        row, point = res
        stocks.append(row)
        if point:
            new_points[ticker] = point

    # ASML carried forward whole (IFRS filer; annual 20-F) — its EUR core
    # P/E inputs stay hand-built, but forward P/E / average P/E and revenue
    # growth are refreshed so the row never freezes at a baked snapshot.
    if "ASML" in prev_by_ticker:
        row = dict(prev_by_ticker["ASML"])
        row["ticker"] = "ASML"
        fpe = sa_forward_pe("ASML")
        if not fpe:
            fpe = yahoo.get("ASML", {}).get("forwardPE")
        if fpe:
            row["forward_pe"] = round(float(fpe), 2)
            if row.get("core_pe"):
                row["avg_pe"] = round((row["core_pe"] + row["forward_pe"]) / 2, 1)
        pts = ((prev_fund.get("stocks") or {}).get("ASML") or {}).get("points") or []
        if len(pts) >= 5 and pts[-1].get("rev_m") and pts[-5].get("rev_m"):
            row["rev_growth_yoy"] = round((pts[-1]["rev_m"] / pts[-5]["rev_m"] - 1) * 100, 1)
        de = asml_debt_equity()
        if de is not None:
            row["debt_equity"] = de
        if "ASML" in prices:
            aprice, adate = prices["ASML"]
            row["price"] = round(aprice, 2)
            row["price_date"] = adate
            row["shares_out"] = ASML_SHARES_OUT
            row["net_debt"] = ASML_NET_DEBT_USD
            mc = aprice * ASML_SHARES_OUT
            row["market_cap_b"] = round(mc / 1e9, 2)
            if row.get("ttm_op_income"):
                row["core_pe"] = round(
                    (mc + ASML_NET_DEBT_USD) / row["ttm_op_income"], 1)
                if row.get("forward_pe"):
                    row["avg_pe"] = round(
                        (row["core_pe"] + row["forward_pe"]) / 2, 1)
            log(f"ASML: price {row['price']} ({adate}); core P/E recomputed")
        stocks.append(row)
        log("ASML: carried forward (IFRS filer); forward P/E + rev growth + D/E refreshed")
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
                      "price": (round(spy_price, 2) if spy_price
                                else (prev_data.get("benchmark") or {}).get("price"))},
    }
    json.dump(data, open(JSON_PATH, "w"), indent=2)
    log(f"wrote {JSON_PATH} as_of={as_of}")
    for s in stocks:
        log(f"  {s['ticker']}: price={s.get('price')} core_pe={s.get('core_pe')} "
            f"fwd={s.get('forward_pe')} rev_yoy={s.get('rev_growth_yoy')}")

    # --- Graph Analysis: append today's closes, recompute SMAs/signals
    if prev_graph:
        update_graph(prev_graph, prices, PRICE_WINDOWS)
        qqq_fpe = sa_qqq_forward_pe()
        if not qqq_fpe:
            qqq_fpe = yahoo.get("QQQ", {}).get("forwardPE")
        if qqq_fpe:
            note = prev_graph.setdefault("notes", {}).setdefault("qqq_forward_pe", {})
            note.update({"value": round(float(qqq_fpe), 2),
                         "source": "stockanalysis.com",
                         "as_of": datetime.date.today().isoformat()})
            log(f"graph: QQQ forward P/E note set to {qqq_fpe}")
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

    # --- Simulation 10Y: rebuild the weekly history file when it changed.
    # Skipped entirely when the stored file already ends at the latest
    # trading day the prices cascade saw (holiday weekdays, reruns).
    try:
        prev_sim10 = load_prev(SIM10_PATH)
        latest_px = max((d for _, d in prices.values()), default=None)
        if prev_sim10.get("dates") and latest_px and prev_sim10["dates"][-1] >= latest_px:
            log("sim10: already ends at the latest trading day; skipped")
        else:
            sim10 = build_sim10(prices)
            if sim10:
                if prev_sim10 != sim10:
                    json.dump(sim10, open(SIM10_PATH, "w"), separators=(",", ":"))
                    log(f"wrote {SIM10_PATH} points={len(sim10['dates'])} last={sim10['dates'][-1]}")
                else:
                    log("sim10: unchanged")
    except Exception as e:
        log(f"sim10: failed ({e}); previous file kept")


if __name__ == "__main__":
    main()
