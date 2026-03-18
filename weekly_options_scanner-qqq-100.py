# region imports
from AlgorithmImports import *
from datetime import datetime, timedelta
# endregion

# ─── Config ───────────────────────────────────────────────────────────────────
WEEKS_OUT  = 4              # target weeks for expiry (~28 days)

TICKERS = [
    "NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "META", "WMT", "GOOGL", "GOOG", "AVGO",
    "MU", "COST", "AMD", "NFLX", "CSCO", "PLTR", "LRCX", "AMAT", "TMUS", "PEP",
    "LIN", "INTC", "TXN", "AMGN", "KLAC", "GILD", "ISRG", "ADI", "HON", "QCOM",
    "SHOP", "BKNG", "VRTX", "ASML", "APP", "PANW", "CMCSA", "INTU", "ADBE", "CRWD",
    "SBUX", "CEG", "MELI", "WDC", "MAR", "STX", "ADP", "REGN", "CDNS", "MDLZ",
    "SNPS", "ORLY", "MNST", "CTAS", "CSX", "WBD", "AEP", "MRVL", "PCAR", "PDD",
    "FTNT", "DASH", "ROST", "NXPI", "BKR", "MPWR", "FAST", "ABNB", "ADSK", "IDXX",
    "EXC", "EA", "XEL", "FANG", "CCEP", "MCHP", "ALNY", "ODFL", "DDOG", "KDP",
    "CPRT", "GEHC", "PYPL", "PAYX", "TTWO", "ROP", "MSTR", "CHTR", "INSM", "WDAY",
    "CTSH", "KHC", "ZS", "DXCM", "VRSK", "CSGP", "ARM", "TEAM",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _is_monthly(dt):
    """True if dt is the 3rd Friday of its month (standard monthly expiry)."""
    if dt.weekday() != 4:
        return False
    return 15 <= dt.day <= 21


class QQQOpenInterestScanner(QCAlgorithm):

    def Initialize(self):
        now = datetime.now().date()
        scan_day = now - timedelta(days=2)
        # Roll back to Friday if landing on a weekend
        while scan_day.weekday() >= 5:
            scan_day -= timedelta(days=1)
        end_day = now - timedelta(days=1)
        self.SetStartDate(scan_day.year, scan_day.month, scan_day.day)
        self.SetEndDate(end_day.year, end_day.month, end_day.day)
        self.SetCash(100_000)

        self._option_symbols = {}

        for ticker in TICKERS:
            eq = self.AddEquity(ticker, Resolution.Hour)
            opt = self.AddOption(ticker, Resolution.Hour)
            opt.SetFilter(lambda u: u.Strikes(-5, +5).Expiration(0, 45))
            self._option_symbols[ticker] = opt.Symbol

        self.Schedule.On(
            self.DateRules.EveryDay(),
            self.TimeRules.BeforeMarketClose("AAPL", 30),
            self._scan,
        )

        self._done = False

    def OnData(self, data):
        pass

    def _scan(self):
        if self._done:
            return
        self._done = True

        today = self.Time.date()
        target_date = today + timedelta(days=WEEKS_OUT * 7)
        results = []

        for ticker in TICKERS:
            opt_sym = self._option_symbols.get(ticker)
            if opt_sym is None:
                continue

            chain = None
            for kvp in self.CurrentSlice.OptionChains:
                if kvp.Key == opt_sym:
                    chain = kvp.Value
                    break
            if chain is None:
                continue

            equity = self.Securities[ticker]
            s_price = equity.Price
            if s_price <= 0:
                continue

            contracts = list(chain)
            if not contracts:
                continue

            # Collect distinct expiry dates and filter to weeklies only
            all_expiries = sorted(set(c.Expiry.date() for c in contracts))
            weekly_expiries = [e for e in all_expiries if not _is_monthly(e)]

            if not weekly_expiries:
                continue

            # Find the weekly expiry closest to WEEKS_OUT weeks from today
            chosen_expiry = min(weekly_expiries, key=lambda e: abs((e - target_date).days))

            # Among contracts at chosen_expiry, find ATM strike
            at_expiry = [c for c in contracts if c.Expiry.date() == chosen_expiry]
            if not at_expiry:
                continue

            strikes = sorted(set(c.Strike for c in at_expiry))
            atm_strike = min(strikes, key=lambda k: abs(k - s_price))

            # Get put and call at ATM strike + chosen expiry
            put_oi   = 0
            call_oi  = 0
            put_vol  = 0
            call_vol = 0

            for c in at_expiry:
                if c.Strike != atm_strike:
                    continue
                if c.Right == OptionRight.Put:
                    put_oi  = int(c.OpenInterest)
                    put_vol = int(c.Volume)
                elif c.Right == OptionRight.Call:
                    call_oi  = int(c.OpenInterest)
                    call_vol = int(c.Volume)

            total_oi = put_oi + call_oi

            results.append({
                "ticker":    ticker,
                "strike":    atm_strike,
                "expiry":    chosen_expiry,
                "put_oi":    put_oi,
                "call_oi":   call_oi,
                "total_oi":  total_oi,
                "put_vol":   put_vol,
                "call_vol":  call_vol,
                "price":     s_price,
            })

        # Sort by total OI descending
        results.sort(key=lambda r: r["total_oi"], reverse=True)

        # Build output
        lines = []
        lines.append(f"QQQ Open Interest Scanner — {today}  (target expiry ~{WEEKS_OUT} weeks out)")
        lines.append(f"Tickers scanned: {len(TICKERS)}  |  Results (weeklies only): {len(results)}")
        lines.append("")
        hdr = (f"{'#':>3} | {'Ticker':<6} | {'Price':>9} | {'Strike':>9} | {'Expiry':>10}"
               f" | {'Put OI':>10} | {'Call OI':>10} | {'Total OI':>10}"
               f" | {'Put Vol':>10} | {'Call Vol':>10}")
        lines.append(hdr)
        lines.append("-" * len(hdr))

        for i, r in enumerate(results, 1):
            lines.append(
                f"{i:>3} | {r['ticker']:<6} | ${r['price']:>8.2f} | ${r['strike']:>8.2f}"
                f" | {r['expiry'].strftime('%Y-%m-%d'):>10}"
                f" | {r['put_oi']:>10,} | {r['call_oi']:>10,} | {r['total_oi']:>10,}"
                f" | {r['put_vol']:>10,} | {r['call_vol']:>10,}"
            )

        lines.append("-" * len(hdr))
        lines.append(f"Total tickers with weekly options: {len(results)}")

        # Log everything
        for line in lines:
            self.Log(line)

        # Persist to ObjectStore
        self.ObjectStore.Save("oi_scan", "\n".join(lines))
        self.Log(f"\nSaved to ObjectStore key 'oi_scan' ({len(lines)} lines)")
