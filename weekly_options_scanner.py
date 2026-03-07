# region imports
from AlgorithmImports import *
from datetime import datetime, timedelta
import calendar
# endregion


# ─── Nasdaq-100 tickers (as of early 2026) ────────────────────────────────────
NASDAQ_100 = [
    "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD",
    "AMGN", "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AZN",
    "BIIB", "BKNG", "BKR", "CDNS", "CDW", "CEG", "CHTR", "CMCSA",
    "COST", "CPRT", "CRWD", "CSCO", "CSGP", "CTAS", "CTSH", "DASH",
    "DDOG", "DLTR", "DXCM", "EA", "EXC", "FANG", "FAST", "FTNT",
    "GEHC", "GFS", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "ILMN",
    "INTC", "INTU", "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX",
    "LULU", "MAR", "MCHP", "MDB", "MDLZ", "MELI", "META", "MNST",
    "MRVL", "MSFT", "MU", "NFLX", "NVDA", "NXPI", "ODFL", "ON",
    "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", "PLTR", "PYPL",
    "QCOM", "REGN", "ROST", "SBUX", "SMCI", "SNPS", "TEAM", "TMUS",
    "TSLA", "TTD", "TTWO", "TXN", "VRSK", "VRTX", "WBD", "WDAY",
    "XEL", "ZS",
]

# ─── Configuration ─────────────────────────────────────────────────────────────
SCAN_DATE = datetime(2026, 1, 5)   # First Monday of Jan 2026


def _third_friday(year, month):
    """Return the date of the third Friday of the given month."""
    # First day of the month
    first_day_weekday = calendar.weekday(year, month, 1)  # 0=Mon … 4=Fri
    # First Friday
    first_friday = 1 + (4 - first_day_weekday) % 7
    third_friday = first_friday + 14
    return datetime(year, month, third_friday).date()


class WeeklyOptionsScanner(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(SCAN_DATE.year, SCAN_DATE.month, SCAN_DATE.day)
        self.SetEndDate(SCAN_DATE.year, SCAN_DATE.month, SCAN_DATE.day + 1)
        self.SetCash(100_000)

        self._equities = {}
        self._options  = {}

        for ticker in NASDAQ_100:
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

        for ticker in NASDAQ_100:
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
                # Is it a Friday? (weekday 4)
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
        self.Log(f"=== Weekly Options Scanner — {SCAN_DATE.date()} ===")
        self.Log(f"Scanned {len(NASDAQ_100)} Nasdaq-100 tickers")
        self.Log(f"Found {len(results)} with weekly options:\n")
        lines.append(f"Weekly Options Scanner — {SCAN_DATE.date()}")
        lines.append(f"Scanned {len(NASDAQ_100)} tickers, {len(results)} have weeklies\n")

        for ticker, mcap, mcap_str in results:
            line = f"{ticker:<8} {mcap_str}"
            self.Log(line)
            lines.append(line)

        # Save to ObjectStore
        self.ObjectStore.Save("weekly_options_scan", "\n".join(lines))
        self.Log(f"\nSaved to ObjectStore key 'weekly_options_scan'")
