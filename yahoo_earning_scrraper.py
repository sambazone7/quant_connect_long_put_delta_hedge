#!/usr/bin/env python3
"""
Yahoo Finance Earnings Calendar Scraper
Scrapes earnings for a date range, filters by market cap, saves to CSV.

Usage:
    python earnings_scraper.py --start 2026-03-13 --end 2026-04-15 --min-market-cap 10

Market cap value is in billions (e.g. 3 = 3B, 10 = 10B, 40 = 40B)
"""

import argparse
import csv
import time
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_market_cap(value: str) -> float:
    """Parse a plain number in billions (e.g. '3', '10', '40') into millions."""
    value = value.strip()
    try:
        num = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid market cap: {value}. Use a number in billions, e.g. 3, 10, 40")
    return num * 1000  # store as millions


def parse_mcap_cell(text: str) -> float:
    """Parse a market cap cell like '1.23B', '450.00M' into millions."""
    if not text or text in ("-", "N/A", ""):
        return 0.0
    text = text.strip().upper().replace(",", "")
    m = re.match(r"([\d.]+)\s*([BMK]?)", text)
    if not m:
        return 0.0
    num, unit = float(m.group(1)), m.group(2)
    if unit == "B":
        return num * 1000
    elif unit == "M":
        return num
    elif unit == "K":
        return num / 1000
    return num


def week_ranges(start: datetime, end: datetime):
    """
    Yield (week_start, week_end) pairs covering start→end.
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


# ── Scraper ────────────────────────────────────────────────────────────────────

def scrape_day(page, week_start: datetime, week_end: datetime, day: datetime) -> list[dict]:
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


def scrape_week(page, week_start: datetime, week_end: datetime) -> list[dict]:
    """Scrape each day in the week individually to capture the expected date."""
    rows_data = []
    day = week_start
    while day <= week_end:
        rows = scrape_day(page, week_start, week_end, day)
        rows_data.extend(rows)
        time.sleep(2)  # polite delay between days
        day += timedelta(days=1)
    return rows_data


def scrape_earnings(start: datetime, end: datetime, min_mcap_m: float,
                    output_file: str, headless: bool = True):
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

        for week_start, week_end in week_ranges(start, end):
            print(f"\nWeek: {fmt(week_start)} → {fmt(week_end)}")
            rows = scrape_week(page, week_start, week_end)
            all_rows.extend(rows)
            time.sleep(2)  # polite delay between weeks

        browser.close()

    # Filter by market cap
    if min_mcap_m > 0:
        before = len(all_rows)
        all_rows = [r for r in all_rows if r["market_cap_m"] >= min_mcap_m]
        print(f"\nFiltered {before} → {len(all_rows)} rows (market cap >= {min_mcap_m:.0f}M)")

    # Deduplicate by symbol + expected_date
    seen = set()
    unique_rows = []
    for r in all_rows:
        key = (r["symbol"], r["expected_date"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    # Save to CSV
    out = Path(output_file)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol", "company", "expected_date", "earnings_date", "market_cap"], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unique_rows)

    print(f"\n✓ Saved {len(unique_rows)} companies to {out.resolve()}")
    return unique_rows


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Yahoo Finance Earnings Scraper")
    parser.add_argument("--start",          required=True,  help="Start date YYYY-MM-DD")
    parser.add_argument("--end",            required=True,  help="End date YYYY-MM-DD")
    parser.add_argument("--min-market-cap", default="0",    help="Min market cap in billions, e.g. 3, 10, 40 (default: no filter)")
    parser.add_argument("--output",         default="earnings.csv", help="Output CSV file (default: earnings.csv)")
    parser.add_argument("--no-headless",    action="store_true",    help="Show browser window (for debugging)")
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

    min_mcap_m = parse_market_cap(args.min_market_cap)

    print(f"Earnings scraper")
    print(f"  Range     : {fmt(start)} → {fmt(end)}")
    print(f"  Min MCap  : {args.min_market_cap}B ({min_mcap_m:.0f}M)")
    print(f"  Output    : {args.output}")
    print(f"  Headless  : {not args.no_headless}")
    print()

    scrape_earnings(
        start       = start,
        end         = end,
        min_mcap_m  = min_mcap_m,
        output_file = args.output,
        headless    = not args.no_headless,
    )


if __name__ == "__main__":
    main()