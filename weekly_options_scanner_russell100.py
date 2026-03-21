# region imports
from AlgorithmImports import *
from datetime import datetime, timedelta
# endregion

# ─── Config ───────────────────────────────────────────────────────────────────
WEEKS_OUT  = 4              # target weeks for expiry (~28 days)

# Russell 2000 (IWM) top 100 by weight, as of Feb 2026
TICKERS = [
    "BE",   "FN",   "NXT",  "KTOS", "CRDO", "SATS", "STRL", "AEIS", "CDE",  "ENSG",
    "HL",   "BBIO", "GH",   "IONQ", "MOD",  "RMBS", "DY",   "TTMI", "SPXC", "GTLS",
    "IDCC", "CTRE", "AHR",  "MDGL", "WTS",  "AVAV", "UMBF", "PRIM", "FLR",  "SITM",
    "VIAV", "ARWR", "FCFS", "ONB",  "FORM", "CYTK", "PRAX", "HQY",  "SM",   "ESE",
    "CMC",  "SMTC", "SANM", "JBTM", "ORA",  "JXN",  "SLAB", "PCVX", "ZWS",  "AXSM",
    "CWAN", "TEX",  "LUMN", "VSAT", "EPRT", "AGX",  "PL",   "EAT",  "KRYS", "APLD",
    "UEC",  "TRNO", "IBP",  "FSS",  "ENS",  "AROC", "AAOI", "NE",   "RIG",  "VAL",
    "OKLO", "POR",  "GATX", "PTGX", "TXNM", "DOCN", "VLY",  "RHP",  "GKOS", "BTSG",
    "SWX",  "TMHC", "KRG",  "MGY",  "UBSI", "GBCI", "CNR",  "ESNT", "PLXS", "NJR",
    "QBTS", "SR",   "BCPC", "ROAD", "CNX",  "NPO",  "CWST", "GVA",  "HWC",  "LNTH",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _is_monthly(dt):
    """True if dt is the 3rd Friday of its month (standard monthly expiry)."""
    if dt.weekday() != 4:
        return False
    return 15 <= dt.day <= 21


class Russell2000OpenInterestScanner(QCAlgorithm):

    def Initialize(self):
        now = datetime.now().date()
        scan_day = now - timedelta(days=2)
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
            self.TimeRules.BeforeMarketClose("BE", 30),
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
        no_weekly = []

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

            all_expiries = sorted(set(c.Expiry.date() for c in contracts))
            weekly_expiries = [e for e in all_expiries if not _is_monthly(e)]

            if not weekly_expiries:
                no_weekly.append(ticker)
                continue

            chosen_expiry = min(weekly_expiries, key=lambda e: abs((e - target_date).days))

            at_expiry = [c for c in contracts if c.Expiry.date() == chosen_expiry]
            if not at_expiry:
                continue

            strikes = sorted(set(c.Strike for c in at_expiry))
            atm_strike = min(strikes, key=lambda k: abs(k - s_price))

            put_oi   = 0
            call_oi  = 0
            put_vol  = 0
            call_vol = 0
            put_bid  = 0.0
            put_ask  = 0.0

            for c in at_expiry:
                if c.Strike != atm_strike:
                    continue
                if c.Right == OptionRight.Put:
                    put_oi  = int(c.OpenInterest)
                    put_vol = int(c.Volume)
                    put_bid = float(c.BidPrice)
                    put_ask = float(c.AskPrice)
                elif c.Right == OptionRight.Call:
                    call_oi  = int(c.OpenInterest)
                    call_vol = int(c.Volume)

            total_oi = put_oi + call_oi
            spread = put_ask - put_bid if put_ask > 0 else 0
            mid = (put_bid + put_ask) / 2 if (put_bid + put_ask) > 0 else 0
            spread_pct = (spread / mid * 100) if mid > 0 else 999

            results.append({
                "ticker":     ticker,
                "strike":     atm_strike,
                "expiry":     chosen_expiry,
                "put_oi":     put_oi,
                "call_oi":    call_oi,
                "total_oi":   total_oi,
                "put_vol":    put_vol,
                "call_vol":   call_vol,
                "price":      s_price,
                "put_bid":    put_bid,
                "put_ask":    put_ask,
                "spread_pct": spread_pct,
            })

        results.sort(key=lambda r: r["total_oi"], reverse=True)

        lines = []
        lines.append(f"Russell 2000 Weekly Options Scanner -- {today}  (target expiry ~{WEEKS_OUT} weeks out)")
        lines.append(f"Tickers scanned: {len(TICKERS)}  |  With weeklies: {len(results)}  |  No weeklies: {len(no_weekly)}")
        lines.append("")
        hdr = (f"{'#':>3} | {'Ticker':<6} | {'Price':>9} | {'Strike':>9} | {'Expiry':>10}"
               f" | {'Put OI':>10} | {'Call OI':>10} | {'Total OI':>10}"
               f" | {'Put Vol':>10} | {'Call Vol':>10}"
               f" | {'Bid':>7} | {'Ask':>7} | {'Sprd%':>6}")
        lines.append(hdr)
        lines.append("-" * len(hdr))

        for i, r in enumerate(results, 1):
            lines.append(
                f"{i:>3} | {r['ticker']:<6} | ${r['price']:>8.2f} | ${r['strike']:>8.2f}"
                f" | {r['expiry'].strftime('%Y-%m-%d'):>10}"
                f" | {r['put_oi']:>10,} | {r['call_oi']:>10,} | {r['total_oi']:>10,}"
                f" | {r['put_vol']:>10,} | {r['call_vol']:>10,}"
                f" | {r['put_bid']:>7.2f} | {r['put_ask']:>7.2f} | {r['spread_pct']:>5.1f}%"
            )

        lines.append("-" * len(hdr))
        lines.append(f"Total tickers with weekly options: {len(results)}")

        if no_weekly:
            lines.append(f"\nTickers with NO weekly expiry found: {', '.join(sorted(no_weekly))}")

        tight = [r for r in results if r["spread_pct"] < 15]
        lines.append(f"\nTickers with put spread < 15%: {len(tight)}")
        for r in tight:
            lines.append(f"  {r['ticker']:<6}  OI={r['total_oi']:>6,}  Spread={r['spread_pct']:.1f}%")

        for line in lines:
            self.Log(line)

        self.ObjectStore.Save("russell_oi_scan", "\n".join(lines))
        self.Log(f"\nSaved to ObjectStore key 'russell_oi_scan' ({len(lines)} lines)")
