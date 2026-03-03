# region imports
from AlgorithmImports import *
# endregion

# ─── Configuration ────────────────────────────────────────────────────────────
N       = 4      # years back from current calendar year (2026 → start 2022)
TICKERS = [
    "AMD", "QCOM", "JNJ", "MPWR", "CSCO", "MRK", "V", "HD", "WMT",
    "GOOGL", "MU", "AVGO", "INTU", "MSFT", "TXN", "BAC", "CVS", "AAPL",
    "AMZN", "NVDA", "TMUS", "INTC", "XOM", "PG", "LLY", "COST", "ADBE",
    "META", "PFE", "PEP", "NFLX", "TSLA", "ABBV", "JPM", "VZ",
]

# ─────────────────────────────────────────────────────────────────────────────

def _quarter(date):
    """Return calendar quarter (1–4) from a date."""
    return (date.month - 1) // 3 + 1


def _week_of_month(date):
    """Return week-of-month index (1–5) using (day-1)//7+1 formula."""
    return (date.day - 1) // 7 + 1


class SEC8KCalendar(QCAlgorithm):

    def Initialize(self):
        current_year = 2026          # update if running in a later year
        start_year   = current_year - N

        today = datetime.now()
        self.SetStartDate(start_year, 1, 1)
        self.SetEndDate(today.year, today.month, today.day)
        self.SetCash(100_000)        # dummy — no trades

        # Per-ticker 8-K date collector: { ticker: [date, ...] }
        self._8k_dates = {t: [] for t in TICKERS}
        self._8k_syms  = {}          # 8-K data symbol → ticker

        for ticker in TICKERS:
            eq_sym  = self.AddEquity(ticker, Resolution.Daily).Symbol
            sec_sym = self.AddData(SECReport8K, eq_sym).Symbol
            self._8k_syms[sec_sym] = ticker

    def OnData(self, data):
        for sec_sym, ticker in self._8k_syms.items():
            if data.ContainsKey(sec_sym):
                self._8k_dates[ticker].append(self.Time.date())

    # ── End-of-backtest report ─────────────────────────────────────────────────

    def OnEndOfAlgorithm(self):
        current_year = 2026
        start_year   = current_year - N
        sep1 = "═" * 68
        sep2 = "─" * 68

        for ticker in TICKERS:
            raw_dates = sorted(set(self._8k_dates.get(ticker, [])))

            # ── Bucket into (year, quarter) → first 8-K per bucket ────────────
            buckets = {}   # (year, q) → first filing date
            for d in raw_dates:
                key = (d.year, _quarter(d))
                if key not in buckets:
                    buckets[key] = d   # keep earliest in the quarter

            # ── Build sorted list of (year, quarter) keys we expect ───────────
            expected = [
                (y, q)
                for y in range(start_year, current_year + 1)
                for q in range(1, 5)
            ]

            # ── Print header ──────────────────────────────────────────────────
            self.Log(sep1)
            self.Log(f"  {ticker}  —  SEC 8-K Quarterly Calendar  |  {start_year}–{current_year}")
            self.Log(sep2)
            self.Log(f"  {'Year':<6} {'Q':<4} {'Filing Date':<14} {'Day':<6} {'Week':<7} {'Δ prev year'}")
            self.Log(sep2)

            missing = []

            for (year, q) in expected:
                filing = buckets.get((year, q))

                if filing is None:
                    missing.append(f"Q{q} {year}")
                    continue

                day_str  = filing.strftime("%a")
                week_str = f"W{_week_of_month(filing)}"

                # Delta vs same quarter previous year (month+day only, ignoring year gap)
                prev = buckets.get((year - 1, q))
                if prev is None or year == start_year:
                    delta_str = "---"
                else:
                    d1    = date(2000, filing.month, filing.day)
                    d0    = date(2000, prev.month,   prev.day)
                    delta = (d1 - d0).days
                    sign  = "+" if delta >= 0 else ""
                    delta_str = f"{sign}{delta}d"

                self.Log(
                    f"  {year:<6} Q{q:<3} {str(filing):<14} {day_str:<6} {week_str:<7} {delta_str}"
                )

            self.Log(sep2)
            if missing:
                self.Log(f"  MISSING quarters: {', '.join(missing)}")
            else:
                self.Log(f"  No missing quarters.")

        self.Log(sep1)
