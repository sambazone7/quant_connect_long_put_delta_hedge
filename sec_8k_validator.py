# region imports
from AlgorithmImports import *
# endregion

# ─── Paste your full MANUAL_EARNINGS_DATES dict here ─────────────────────────
MANUAL_EARNINGS_DATES = {
    "AAPL": [
        datetime(2022, 1, 27), datetime(2022, 4, 28), datetime(2022, 7, 28), datetime(2022, 10, 27),
        datetime(2023, 2, 2),  datetime(2023, 5, 4),  datetime(2023, 8, 3),  datetime(2023, 11, 2),
        datetime(2024, 2, 1),  datetime(2024, 5, 2),  datetime(2024, 8, 1),  datetime(2024, 10, 31),
        datetime(2025, 1, 30), datetime(2025, 5, 1),  datetime(2025, 7, 31), datetime(2026, 1, 29),
    ],
    "AMZN": [
        datetime(2022, 2, 3),  datetime(2022, 7, 28), datetime(2022, 10, 27),  # Removed Apr 2022 (Split Jun)
        datetime(2023, 2, 2),  datetime(2023, 4, 27), datetime(2023, 8, 3),  datetime(2023, 10, 26),
        datetime(2024, 2, 1),  datetime(2024, 4, 30), datetime(2024, 8, 1),  datetime(2024, 10, 31),
        datetime(2025, 2, 6),  datetime(2025, 5, 1),  datetime(2025, 7, 31), datetime(2026, 2, 5),
    ],
}

# ─── Validation settings ──────────────────────────────────────────────────────
MATCH_WINDOW_DAYS = 7   # search for nearest 8-K within ±N days of manual date
WARN_THRESHOLD    = 4   # diff (days) above this → WARN; negative → FLAG

# ─────────────────────────────────────────────────────────────────────────────

class SECEarningsValidator(QCAlgorithm):

    def Initialize(self):
        # Derive date range from the dates dict (with a small buffer)
        all_dates = [d for dates in MANUAL_EARNINGS_DATES.values() for d in dates]
        start = min(all_dates) - timedelta(days=10)
        end   = max(all_dates) + timedelta(days=10)
        self.SetStartDate(start.year, start.month, start.day)
        self.SetEndDate(end.year,   end.month,   end.day)
        self.SetCash(100_000)  # dummy — no trades

        # Per-ticker 8-K date collector:  { ticker: [date, ...] }
        self._8k_dates  = {}
        self._8k_syms   = {}   # map 8-K data symbol → ticker

        for ticker in MANUAL_EARNINGS_DATES:
            eq_sym = self.AddEquity(ticker, Resolution.Daily).Symbol
            sec_sym = self.AddData(SECReport8K, eq_sym).Symbol
            self._8k_dates[ticker] = []
            self._8k_syms[sec_sym]  = ticker

    def OnData(self, data):
        for sec_sym, ticker in self._8k_syms.items():
            if data.ContainsKey(sec_sym):
                filing_date = self.Time.date()
                self._8k_dates[ticker].append(filing_date)

    def OnEndOfAlgorithm(self):
        sep  = "─" * 72
        sep2 = "═" * 72

        for ticker, manual_dates in MANUAL_EARNINGS_DATES.items():
            filings = sorted(self._8k_dates.get(ticker, []))

            self.Log(sep2)
            self.Log(f"  {ticker}  |  {len(manual_dates)} manual date(s)  |  {len(filings)} 8-K filing(s) found")
            self.Log(sep)

            ok      = 0
            warn    = 0
            flag    = 0
            missing = 0

            for md in sorted(manual_dates):
                md_date = md.date() if isinstance(md, datetime) else md

                # Find nearest 8-K within ±MATCH_WINDOW_DAYS
                best      = None
                best_diff = None
                for fd in filings:
                    diff = (fd - md_date).days
                    if abs(diff) <= MATCH_WINDOW_DAYS:
                        if best_diff is None or abs(diff) < abs(best_diff):
                            best      = fd
                            best_diff = diff

                if best is None:
                    status = "MISSING ← FLAG"
                    missing += 1
                    self.Log(f"  {ticker}  announced: {md_date}  8-K: {'none found':<12}  diff:      {status}")
                else:
                    if best_diff < 0:
                        status = "FLAG ← our date is LATE (8-K came before)"
                        flag  += 1
                    elif best_diff > WARN_THRESHOLD:
                        status = f"WARN (>{WARN_THRESHOLD}d gap)"
                        warn  += 1
                    else:
                        status = "OK"
                        ok    += 1
                    sign = "+" if best_diff >= 0 else ""
                    self.Log(f"  {ticker}  announced: {md_date}  8-K: {str(best):<12}  diff: {sign}{best_diff}d  {status}")

            self.Log(sep)
            self.Log(f"  {ticker}  Summary: OK={ok}  WARN={warn}  FLAG={flag}  MISSING={missing}")

        self.Log(sep2)
