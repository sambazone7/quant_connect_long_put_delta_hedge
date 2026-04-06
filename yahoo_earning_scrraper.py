#!/usr/bin/env python3
"""
Yahoo Finance Earnings Calendar Scraper
Scrapes earnings for a date range, filters by market cap, saves to CSV.

Usage:
    python earnings_scraper.py --start 2026-03-13 --end 2026-04-15 --min-market-cap 10

Market cap value is in billions (e.g. 3 = 3B, 10 = 10B, 40 = 40B)
"""

import argparse
import concurrent.futures
import csv
import time
import re
import sys
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

try:
    import yfinance as yf
except Exception:
    yf = None

# yfinance HTTP has no built-in timeout; hung sockets look like a "stuck" scraper.
DEFAULT_YF_CALL_TIMEOUT_SEC = 25.0

# Brief pause between tickers during enrichment to ease Yahoo / curl pressure.
ENRICHMENT_BACKOFF_SEC = 0.15

EARNINGS_CSV_FIELDNAMES = [
    "symbol",
    "company",
    "sector",
    "industry",
    "expected_date",
    "earnings_date",
    "market_cap",
    "exchange",
    "consistency_score_exact",
    "consistency_score_within1",
]

_ENRICHMENT_COLUMN_KEYS = (
    "sector",
    "industry",
    "exchange",
    "consistency_score_exact",
    "consistency_score_within1",
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def ensure_enrichment_columns(row: dict) -> None:
    """Ensure CSV enrichment columns exist (e.g. after partial run or interrupt)."""
    for k in _ENRICHMENT_COLUMN_KEYS:
        row.setdefault(k, "")


def write_earnings_csv(output_file: str, rows: list[dict]) -> Path:
    """Write earnings rows to CSV; fills missing enrichment keys with empty strings."""
    out = Path(output_file)
    for row in rows:
        ensure_enrichment_columns(row)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=EARNINGS_CSV_FIELDNAMES,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)
    return out


def _run_with_timeout(label: str, ticker: str, timeout_sec: float, func):
    """
    Run func() in a worker thread and return its result.
    On timeout or exception, print a warning and return None.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(func)
        try:
            return fut.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            print(f"    [WARN] {label} timed out after {timeout_sec:.0f}s for {ticker}")
            return None
        except Exception as exc:
            print(f"    [WARN] {label} failed for {ticker}: {exc}")
            return None

def parse_market_cap(value: str) -> float:
    """Parse a plain number in billions (e.g. '3', '10', '40') into millions."""
    value = value.strip()
    try:
        num = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid market cap: {value}. Use a number in billions, e.g. 3, 10, 40")
    return num * 1000  # store as millions


def parse_mcap_cell(text: str) -> float:
    """Parse a market cap cell like '1.98T', '1.23B', '450.00M' into millions."""
    if not text or text in ("-", "N/A", ""):
        return 0.0
    text = text.strip().upper().replace(",", "")
    m = re.match(r"([\d.]+)\s*([TBMK]?)", text)
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2)
    if unit == "T":
        return num * 1_000_000
    elif unit == "B":
        return num * 1000
    elif unit == "M":
        return num
    elif unit == "K":
        return num / 1000
    return num


def week_ranges(start: datetime, end: datetime):
    """
    Yield (week_start, week_end) pairs covering start->end.
    Yahoo moves in 1-week windows (Sunday to Saturday).
    """
    # Align to Yahoo's week: find the Sunday on or before start
    cursor = start - timedelta(days=start.weekday() + 1)  # Monday - 1 = Sunday
    if cursor > start:
        cursor = start

    while cursor <= end:
        week_end = cursor + timedelta(days=6)
        yield cursor, min(week_end, end)
        cursor += timedelta(days=7)


def fmt(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def quarter_of_date(d: datetime) -> int:
    return ((d.month - 1) // 3) + 1


def parse_date_safe(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        return None


def round_half_up(value: float) -> int:
    """Round halves up (e.g. 17.5 -> 18)."""
    return int(value + 0.5)


def expected_week_from_samples(weeks: list[int]) -> int | None:
    """Get expected week from mode; tie-break by median of tied weeks."""
    if not weeks:
        return None
    counts = Counter(weeks)
    top_freq = max(counts.values())
    tied_weeks = sorted([w for w, freq in counts.items() if freq == top_freq])
    if len(tied_weeks) == 1:
        return tied_weeks[0]
    mid = len(tied_weeks) // 2
    if len(tied_weeks) % 2 == 1:
        return tied_weeks[mid]
    return round_half_up((tied_weeks[mid - 1] + tied_weeks[mid]) / 2.0)


def extract_earnings_datetimes(df) -> list[datetime]:
    """Extract datetime values from yfinance earnings dates dataframe."""
    if df is None or len(df) == 0:
        return []
    dates = []

    if hasattr(df, "index"):
        for value in df.index:
            try:
                dt = value.to_pydatetime() if hasattr(value, "to_pydatetime") else None
                if dt is None:
                    continue
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                dates.append(dt)
            except Exception:
                continue

    if dates:
        return dates

    for col in ("Earnings Date", "Date", "earningsDate"):
        if col not in df.columns:
            continue
        for value in df[col].dropna():
            try:
                dt = value.to_pydatetime() if hasattr(value, "to_pydatetime") else None
                if dt is None:
                    continue
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                dates.append(dt)
            except Exception:
                continue
        if dates:
            break

    return dates


def fetch_quarter_dates(
    symbol: str,
    history_cache: dict[str, dict[int, list[datetime]]],
    yf_timeout_sec: float,
) -> dict[int, list[datetime]]:
    """Fetch and cache earnings release datetimes by quarter."""
    ticker = symbol.strip().upper()
    if not ticker:
        return {1: [], 2: [], 3: [], 4: []}
    if ticker in history_cache:
        return history_cache[ticker]

    quarter_dates = {1: [], 2: [], 3: [], 4: []}
    if yf is None:
        print(f"    [WARN] yfinance not available; consistency scores unavailable for {ticker}")
        history_cache[ticker] = quarter_dates
        return quarter_dates

    def load_earnings_dates():
        return yf.Ticker(ticker).get_earnings_dates(limit=20)

    df = _run_with_timeout(
        "Earnings history fetch", ticker, yf_timeout_sec, load_earnings_dates
    )
    if df is not None:
        try:
            seen_dates = set()
            for dt in extract_earnings_datetimes(df):
                date_key = dt.date()
                if date_key in seen_dates:
                    continue
                seen_dates.add(date_key)
                q = quarter_of_date(dt)
                quarter_dates[q].append(dt)
        except Exception as exc:
            print(f"    [WARN] Could not parse earnings history for {ticker}: {exc}")

    for q in quarter_dates:
        quarter_dates[q].sort(reverse=True)

    history_cache[ticker] = quarter_dates
    return quarter_dates


def compute_week_consistency_scores(
    symbol: str,
    expected_date: str,
    history_cache: dict[str, dict[int, list[datetime]]],
    score_cache: dict[tuple[str, int, str], tuple[str, str]],
    yf_timeout_sec: float,
) -> tuple[str, str]:
    """Compute exact-week and +/-1-week consistency percentages."""
    dt = parse_date_safe(expected_date)
    ticker = symbol.strip().upper()
    if not dt or not ticker:
        return "", ""

    quarter = quarter_of_date(dt)
    cache_key = (ticker, quarter, expected_date)
    if cache_key in score_cache:
        return score_cache[cache_key]

    quarter_dates = fetch_quarter_dates(ticker, history_cache, yf_timeout_sec)
    dates = [d for d in quarter_dates.get(quarter, []) if d.date() < dt.date()]

    # Keep at most 1 sample per year, most-recent first, up to 5 years.
    picked = []
    used_years = set()
    for d in dates:
        if d.year in used_years:
            continue
        used_years.add(d.year)
        picked.append(d)
        if len(picked) == 5:
            break
    weeks = [int(d.isocalendar()[1]) for d in picked]

    # Use a minimum sample size to avoid noisy percentages.
    if len(weeks) < 3:
        result = ("", "")
        score_cache[cache_key] = result
        return result

    expected_week = expected_week_from_samples(weeks)
    if expected_week is None:
        result = ("", "")
        score_cache[cache_key] = result
        return result

    week_diff = [w - expected_week for w in weeks]
    exact = 100.0 * sum(1 for d in week_diff if abs(d) == 0) / len(week_diff)
    within1 = 100.0 * sum(1 for d in week_diff if abs(d) <= 1) / len(week_diff)
    result = (f"{exact:.1f}%", f"{within1:.1f}%")
    score_cache[cache_key] = result
    return result


def normalize_exchange(raw_exchange: str) -> str:
    """Normalize Yahoo exchange labels into NASDAQ / NYSE / OTHER."""
    if not raw_exchange:
        return "OTHER"
    raw = raw_exchange.strip().upper()

    if "NASDAQ" in raw or raw in {"NMS", "NGM", "NCM"}:
        return "NASDAQ"
    if "NYSE" in raw or raw in {"NYQ", "NYE", "NYS"}:
        return "NYSE"
    return "OTHER"


def resolve_exchange(symbol: str, exchange_cache: dict[str, str]) -> str:
    """Lookup ticker exchange via Yahoo finance search endpoint with cache."""
    ticker = symbol.strip().upper()
    if not ticker:
        return "OTHER"
    if ticker in exchange_cache:
        return exchange_cache[ticker]

    params = urlencode({"q": ticker, "quotesCount": 10, "newsCount": 0})
    url = f"https://query1.finance.yahoo.com/v1/finance/search?{params}"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    exchange = "OTHER"
    try:
        with urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        quotes = data.get("quotes", [])

        # Prefer exact symbol matches to avoid picking a similarly named ticker.
        candidates = [q for q in quotes if str(q.get("symbol", "")).upper() == ticker]
        chosen = candidates[0] if candidates else (quotes[0] if quotes else {})
        raw_exchange = (
            chosen.get("exchangeDisp")
            or chosen.get("exchDisp")
            or chosen.get("exchange")
            or chosen.get("fullExchangeName")
            or ""
        )
        exchange = normalize_exchange(str(raw_exchange))
    except Exception as exc:
        print(f"    [WARN] Exchange lookup failed for {ticker}: {exc}")

    exchange_cache[ticker] = exchange
    return exchange


def resolve_sector_industry(
    symbol: str,
    sector_industry_cache: dict[str, tuple[str, str]],
    yf_timeout_sec: float,
) -> tuple[str, str]:
    """Lookup sector and industry via yfinance quote info; empty strings if unavailable."""
    ticker = symbol.strip().upper()
    if not ticker:
        return ("", "")
    if ticker in sector_industry_cache:
        return sector_industry_cache[ticker]

    if yf is None:
        sector_industry_cache[ticker] = ("", "")
        return ("", "")

    def load_info():
        return yf.Ticker(ticker).info or {}

    info = _run_with_timeout(
        "Sector/industry lookup", ticker, yf_timeout_sec, load_info
    )
    if info is None:
        info = {}
    sector = (info.get("sector") or "").strip()
    industry = (info.get("industry") or "").strip()

    sector_industry_cache[ticker] = (sector, industry)
    return (sector, industry)


def accept_yahoo_cookies_once(page, consent_state: dict):
    """Attempt a one-time click on Yahoo's cookie consent accept button."""
    if consent_state.get("handled"):
        return
    consent_state["handled"] = True

    selectors = [
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button:has-text('I agree')",
        "button:has-text('Agree')",
        "#consent-page button:has-text('Accept all')",
        "[data-testid='consent'] button:has-text('Accept all')",
    ]

    # Yahoo may render consent either in the main page or in a consent frame.
    contexts = [page] + list(page.frames)
    for ctx in contexts:
        for selector in selectors:
            try:
                btn = ctx.locator(selector).first
                if btn.is_visible(timeout=1200):
                    btn.click(timeout=2000)
                    print("    Accepted Yahoo cookies.")
                    time.sleep(1)
                    return
            except Exception:
                continue

    print("    Cookie prompt not found (or already accepted).")


# ── Scraper ────────────────────────────────────────────────────────────────────

def scrape_day(page, week_start: datetime, week_end: datetime, day: datetime, consent_state: dict) -> list[dict]:
    """Scrape earnings for a single day within a week window."""
    url = (
        f"https://finance.yahoo.com/calendar/earnings/"
        f"?from={fmt(week_start)}&to={fmt(week_end)}&day={fmt(day)}"
    )
    print(f"  Fetching: {url}")

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PlaywrightTimeout:
        print(f"  [WARN] Timeout loading {url}, skipping day.")
        return []
    accept_yahoo_cookies_once(page, consent_state)

    # Wait for table or detect empty state
    try:
        page.wait_for_selector("table, [data-testid='table-container'], .no-results", timeout=15000)
    except PlaywrightTimeout:
        print(f"  [WARN] No table found for {fmt(day)}, skipping.")
        return []

    rows_data = []

    # Set rows-per-page to 100 — Yahoo uses a custom button+menu
    try:
        page.wait_for_selector("table tbody tr", timeout=10000)
        clicked = False

        # Wait a bit for the pagination controls to render
        time.sleep(2)

        # Click the "25" button next to "Rows per page" label
        triggered = page.evaluate("""
            () => {
                const btn = [...document.querySelectorAll('button')]
                    .find(b => b.innerText.trim() === '25');
                if (btn) { btn.click(); return true; }
                return false;
            }
        """)

        if triggered:
            time.sleep(0.8)
            selected = page.evaluate("""
                () => {
                    const opt = [...document.querySelectorAll('li, [role="option"], button')]
                        .find(el => el.innerText.trim() === '100');
                    if (opt) { opt.click(); return true; }
                    return false;
                }
            """)
            if selected:
                print(f"    Set rows per page to 100")
                time.sleep(2)
                clicked = True

        if not clicked:
            print(f"    [WARN] Could not set rows-per-page dropdown, staying at 25")
    except Exception as e:
        print(f"    [WARN] Could not set rows per page: {e}")

    # Now read all rows (single page)
    try:
        page.wait_for_selector("table tbody tr", timeout=10000)
    except PlaywrightTimeout:
        print("    no rows.")
        return []

    rows = page.query_selector_all("table tbody tr")
    print(f"    {fmt(day)}: {len(rows)} rows")

    expected_date = fmt(day)

    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) < 3:
            continue
        def cell_text(i, _cells=cells):
            return _cells[i].inner_text().strip() if i < len(_cells) else ""
        # Scan from the end for the first cell that looks like a market cap
        mcap_raw = "-"
        for idx in range(len(cells) - 1, -1, -1):
            val = cell_text(idx)
            if re.search(r"[\d.]+\s*[BMTKbmtk]", val):
                mcap_raw = val
                break
        mcap_m = parse_mcap_cell(mcap_raw)
        mcap_b = f"{mcap_m / 1000:.0f}B" if mcap_m > 0 else "-"
        rows_data.append({
            "symbol":        cell_text(0),
            "company":       cell_text(1),
            "earnings_date": cell_text(2),
            "expected_date": expected_date,
            "market_cap":    mcap_b,
            "market_cap_m":  mcap_m,
        })

    print(f"  Total rows collected for {fmt(day)}: {len(rows_data)}")

    return rows_data


def scrape_week(page, week_start: datetime, week_end: datetime, consent_state: dict) -> list[dict]:
    """Scrape each day in the week individually to capture the expected date."""
    rows_data = []
    day = week_start
    while day <= week_end:
        rows = scrape_day(page, week_start, week_end, day, consent_state)
        rows_data.extend(rows)
        time.sleep(2)  # polite delay between days
        day += timedelta(days=1)
    return rows_data


def scrape_earnings(
    start: datetime,
    end: datetime,
    min_mcap_m: float,
    output_file: str,
    headless: bool = True,
    yf_timeout_sec: float = DEFAULT_YF_CALL_TIMEOUT_SEC,
):
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()
        consent_state = {"handled": False}

        for week_start, week_end in week_ranges(start, end):
            print(f"\nWeek: {fmt(week_start)} -> {fmt(week_end)}")
            rows = scrape_week(page, week_start, week_end, consent_state)
            all_rows.extend(rows)
            time.sleep(2)  # polite delay between weeks

        browser.close()

    # Filter by market cap
    if min_mcap_m > 0:
        before = len(all_rows)
        all_rows = [r for r in all_rows if r["market_cap_m"] >= min_mcap_m]
        print(f"\nFiltered {before} -> {len(all_rows)} rows (market cap >= {min_mcap_m:.0f}M)")

    # Deduplicate by symbol + expected_date
    seen = set()
    unique_rows = []
    for r in all_rows:
        key = (r["symbol"], r["expected_date"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    # Enrich rows with sector/industry, exchange, and week-consistency scores.
    exchange_cache = {}
    sector_industry_cache: dict[str, tuple[str, str]] = {}
    history_cache = {}
    score_cache = {}
    n_rows = len(unique_rows)
    print(
        f"\nEnriching {n_rows} row(s) "
        f"(Yahoo Finance lookups, max {yf_timeout_sec:.0f}s each)..."
    )
    interrupted = False
    try:
        for i, row in enumerate(unique_rows, 1):
            sym = row.get("symbol", "")
            if n_rows <= 30 or i == 1 or i == n_rows or i % 10 == 0:
                print(f"  [{i}/{n_rows}] {sym}")
            sec, ind = resolve_sector_industry(
                row.get("symbol", ""), sector_industry_cache, yf_timeout_sec
            )
            row["sector"] = sec
            row["industry"] = ind
            row["exchange"] = resolve_exchange(row.get("symbol", ""), exchange_cache)
            exact_score, within1_score = compute_week_consistency_scores(
                row.get("symbol", ""),
                row.get("expected_date", ""),
                history_cache,
                score_cache,
                yf_timeout_sec,
            )
            row["consistency_score_exact"] = exact_score
            row["consistency_score_within1"] = within1_score
            if i < n_rows:
                time.sleep(ENRICHMENT_BACKOFF_SEC)
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted — writing partial CSV with rows processed so far...")

    out = write_earnings_csv(output_file, unique_rows)
    if interrupted:
        print(f"\nSaved {len(unique_rows)} companies (partial) to {out.resolve()}")
        raise SystemExit(130)
    print(f"\nSaved {len(unique_rows)} companies to {out.resolve()}")
    return unique_rows


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Yahoo Finance Earnings Scraper")
    parser.add_argument("--start",          required=True,  help="Start date YYYY-MM-DD")
    parser.add_argument("--end",            required=True,  help="End date YYYY-MM-DD")
    parser.add_argument("--min-market-cap", default="0",    help="Min market cap in billions, e.g. 3, 10, 40 (default: no filter)")
    parser.add_argument("--output",         default="earnings.csv", help="Output CSV file (default: earnings.csv)")
    parser.add_argument("--no-headless",    action="store_true",    help="Show browser window (for debugging)")
    parser.add_argument(
        "--yf-timeout",
        type=float,
        default=DEFAULT_YF_CALL_TIMEOUT_SEC,
        metavar="SEC",
        help=(
            "Max seconds per yfinance call (sector/industry, earnings history); "
            f"default {DEFAULT_YF_CALL_TIMEOUT_SEC:.0f}"
        ),
    )
    args = parser.parse_args()

    try:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end   = datetime.strptime(args.end,   "%Y-%m-%d")
    except ValueError:
        print("ERROR: Dates must be in YYYY-MM-DD format")
        sys.exit(1)

    if start > end:
        print("ERROR: --start must be before --end")
        sys.exit(1)

    if args.yf_timeout <= 0:
        print("ERROR: --yf-timeout must be positive")
        sys.exit(1)

    min_mcap_m = parse_market_cap(args.min_market_cap)

    print(f"Earnings scraper")
    print(f"  Range     : {fmt(start)} -> {fmt(end)}")
    print(f"  Min MCap  : {args.min_market_cap}B ({min_mcap_m:.0f}M)")
    print(f"  Output    : {args.output}")
    print(f"  Headless  : {not args.no_headless}")
    print(f"  YF timeout: {args.yf_timeout}s per call")
    print()

    scrape_earnings(
        start            = start,
        end              = end,
        min_mcap_m       = min_mcap_m,
        output_file      = args.output,
        headless         = not args.no_headless,
        yf_timeout_sec   = args.yf_timeout,
    )


if __name__ == "__main__":
    main()