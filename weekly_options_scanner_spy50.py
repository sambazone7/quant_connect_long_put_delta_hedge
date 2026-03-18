# region imports
from AlgorithmImports import *
from datetime import datetime, timedelta
import calendar
# endregion


# ─── Top 50 S&P 500 companies by market cap (as of early 2026) ───────────────
SPY_TOP50 = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "BRK.B",
    "LLY", "AVGO", "JPM", "TSLA", "WMT", "UNH", "XOM", "V", "MA",
    "PG", "COST", "JNJ", "HD", "ORCL", "ABBV", "BAC", "NFLX",
    "MRK", "KO", "CRM", "CVX", "ADBE", "AMD", "PEP", "TMO", "LIN",
    "CSCO", "ACN", "MCD", "ABT", "WFC", "IBM", "PM", "GE", "ISRG",
    "INTU", "NOW", "CAT", "GS", "QCOM", "TXN", "AMGN",
]

# ─── Configuration ─────────────────────────────────────────────────────────────
SCAN_DATE = datetime(2026, 1, 5)   # First Monday of Jan 2026


def _third_friday(year, month):
    """Return the date of the third Friday of the given month."""
    first_day_weekday = calendar.weekday(year, month, 1)  # 0=Mon … 4=Fri
    first_friday = 1 + (4 - first_day_weekday) % 7
    third_friday = first_friday + 14
    return datetime(year, month, third_friday).date()


class WeeklyOptionsScannerSPY50(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(SCAN_DATE.year, SCAN_DATE.month, SCAN_DATE.day)
        self.SetEndDate(SCAN_DATE.year, SCAN_DATE.month, SCAN_DATE.day + 1)
        self.SetCash(100_000)

        self._equities = {}
        self._options  = {}

        for ticker in SPY_TOP50:
            eq = self.AddEquity(ticker, Resolution.Daily)
            eq.SetDataNormalizationMode(DataNormalizationMode.Raw)
            self._equities[ticker] = eq.Symbol

            opt = self.AddOption(ticker, Resolution.Daily)
            opt.SetFilter(lambda u: u.Strikes(-1, +1)
                                     .Expiration(timedelta(0), timedelta(45))
                                     .PutsOnly())
            self._options[ticker] = opt.Symbol

        self._scanned = False

    def OnData(self, data):
        if self._scanned:
            return
        self._scanned = True

        results = []

        for ticker in SPY_TOP50:
            opt_sym = self._options[ticker]

            chain = data.OptionChains.get(opt_sym)
            if chain is None:
                self.Debug(f"  {ticker}: no option chain data")
                continue

            # Collect unique expiration dates
            expirations = set()
            for contract in chain:
                expirations.add(contract.Expiry.date())

            if not expirations:
                self.Debug(f"  {ticker}: empty chain")
                continue

            # Check for weekly expirations (any Friday that is NOT the 3rd Friday)
            has_weekly = False
            for exp in sorted(expirations):
                if exp.weekday() != 4:
                    continue
                tf = _third_friday(exp.year, exp.month)
                if exp != tf:
                    has_weekly = True
                    break

            if not has_weekly:
                continue

            # Get market cap
            eq_sym = self._equities[ticker]
            price  = self.Securities[eq_sym].Price
            mcap   = 0.0
            if self.Securities[eq_sym].Fundamentals is not None:
                try:
                    mcap = self.Securities[eq_sym].Fundamentals.MarketCap
                except Exception:
                    pass

            if mcap > 0:
                mcap_str = f"${mcap / 1e9:,.1f}B"
            else:
                mcap_str = f"price=${price:,.2f} (no mcap data)"

            results.append((ticker, mcap, mcap_str))

        # Sort by market cap descending
        results.sort(key=lambda x: -x[1])

        # Output
        lines = []
        self.Log(f"=== Weekly Options Scanner (S&P 500 Top 50) — {SCAN_DATE.date()} ===")
        self.Log(f"Scanned {len(SPY_TOP50)} S&P 500 top-50 tickers")
        self.Log(f"Found {len(results)} with weekly options:\n")
        lines.append(f"Weekly Options Scanner (S&P 500 Top 50) — {SCAN_DATE.date()}")
        lines.append(f"Scanned {len(SPY_TOP50)} tickers, {len(results)} have weeklies\n")

        for ticker, mcap, mcap_str in results:
            line = f"{ticker:<8} {mcap_str}"
            self.Log(line)
            lines.append(line)

        # Save to ObjectStore
        self.ObjectStore.Save("weekly_options_scan_spy50", "\n".join(lines))
        self.Log(f"\nSaved to ObjectStore key 'weekly_options_scan_spy50'")
