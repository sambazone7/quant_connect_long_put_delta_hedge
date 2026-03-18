# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import requests
import math
# endregion

# ─── Configurable Parameters ──────────────────────────────────────────────────
 
N      = 16       # Number of past earnings events to backtest (most recent N)
K              = 14     # Fixed entry day (trading days before earnings)
                        # Also used as scan start when DYNAMIC_ENTRY = True
DYNAMIC_ENTRY  = False  # False → enter at exactly K days (original behaviour)
                        # True  → scan from K days, enter on vega/theta signal
M              = 5.0    # (DYNAMIC_ENTRY only — legacy, unused with new IV-ratio trigger)
W              = 1.15   # (DYNAMIC_ENTRY only) enter when current_IV / 6-day-avg_IV >= W
                        # e.g. 1.15 → enter when IV has risen 15% above its baseline average
                        # e.g. M=5 → fire when IV expansion gain ≥ 5 days of theta cost (~5pp IV rise)
S              = 10_000   # Notional USD value of puts to buy at entry
D_mult  = 1.0     # Hedge band = D_mult × implied daily 1-sigma move
                  # e.g. 1.0 → hedge every 1 daily sigma; 2.0 → every 2 sigma
D_floor = 0.5     # Minimum trigger % (guard against near-zero IV)
D_ceil  = 5.0     # Maximum trigger % (guard against extreme IV spikes)
F      = 0        # Minimum calendar days after earnings date for put expiry
                  # F=0 → earliest available expiry on/after earnings (default)
                  # F=7 → expiry must be at least 7 days after earnings date
Z      = 0.0      # IV/RV filter: skip entry if IV/RV >= Z
PRICE_MODEL = "BT"   # Option pricing model for Greeks: "BT" | "BS" | "default"
                     # BT  = Binomial CoxRossRubinstein (American equity options — recommended)
                     # BS  = Black-Scholes (European-style, faster, ignores early exercise)
                     # default = QC built-in (no explicit model set)
                  # Z=0.0 → disabled (always enter regardless of IV/RV)
                  # Z=1.0 → only enter when IV is below 30-day realized vol
                  # Z=1.5 → only enter when IV < 1.5× realized vol

# ─── Financial Modeling Prep API ──────────────────────────────────────────────
FMP_API_KEY = ""   # Leave empty to rely solely on MANUAL_EARNINGS_DATES below

# ─── Earnings Dates ───────────────────────────────────────────────────────────
# ← CHANGE: Replace with the last 8 earnings announcement dates for TICKER
# Format: datetime(YYYY, M, D)  — use the announcement date (not fiscal period end)
 

TICKER = "TMUS"

MANUAL_EARNINGS_DATES = [
    datetime(2022, 2, 2),    # Q4 FY2021  EPS: $1.10
    datetime(2022, 4, 27),   # Q1 FY2022  EPS: $0.57
    datetime(2022, 7, 27),   # Q2 FY2022  EPS: $1.43 (Massive beat)
    datetime(2022, 10, 27),  # Q3 FY2022  EPS: $0.40
    datetime(2023, 2, 1),    # Q4 FY2022  EPS: $1.18
    datetime(2023, 4, 27),   # Q1 FY2023  EPS: $1.58
    datetime(2023, 7, 27),   # Q2 FY2023  EPS: $1.86
    datetime(2023, 10, 25),  # Q3 FY2023  EPS: $1.82
    datetime(2024, 1, 25),   # Q4 FY2023  EPS: $1.67
    datetime(2024, 4, 25),   # Q1 FY2024  EPS: $2.00
    datetime(2024, 7, 31),   # Q2 FY2024  EPS: $2.49
    datetime(2024, 10, 23),  # Q3 FY2024  EPS: $2.61
    datetime(2025, 1, 29),   # Q4 FY2024  EPS: $2.57
    datetime(2025, 4, 23),   # Q1 FY2025  EPS: $2.58
    datetime(2025, 7, 22),   # Q2 FY2025  EPS: $2.84
    datetime(2025, 10, 23),  # Q3 FY2025  EPS: $2.59
    datetime(2026, 2, 11),   # Q4 FY2025  EPS: $2.14 (Most Recent)
]
def _fetch_earnings_fmp(ticker: str, n: int, api_key: str, start_date: Date, end_date: Date):
    """
    Fetch up to *n* quarterly earnings announcement dates for *ticker* that fall
    within [start_date, end_date] using the FMP stable/earnings endpoint (free tier).

    Free plan caps limit at 5 — sufficient for a 1-year backtest with N≤4.
    Raises requests.HTTPError on a bad HTTP response.
    """
    limit = 5
    url = (
        f"https://financialmodelingprep.com/stable/earnings"
        f"?symbol={ticker}&limit={limit}&apikey={api_key}"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"FMP returned unexpected payload: {data}")

    dates = []
    for entry in data:
        raw = entry.get("date", "")
        if not raw or entry.get("epsActual") is None:
            continue
        try:
            d = datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            continue
        if start_date <= d.date() <= end_date:
            dates.append(d)

    return sorted(set(dates))


class EarningsLongPutDeltaNeutral(QCAlgorithm):
    """
    Generic Earnings Long-Put + Delta-Neutral Strategy
    ===================================================
    K trading days before each of the last N earnings dates:
        1. Buy puts (notional ≈ $S) expiring at least F days after earnings,
           ATM / slightly OTM.
        2. Buy stock to make the combined position delta-neutral.

    Daily (30 min before close):
        Re-hedge stock quantity if stock has moved ≥ D% since last hedge.
        Report: stock PnL, put PnL, combined PnL, current put IV.

    1 trading day before earnings:
        Close put first → then close stock.
        Report final PnL breakdown + IV at entry vs exit.

    All fills execute at (bid + ask) / 2 via MidPriceFillModel.
    """

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(1_000_000)

        # (startup log suppressed — see OnEndOfAlgorithm for summary)

        # ── Equity ────────────────────────────────────────────────────────────
        self.stock_symbol = self.AddEquity(TICKER, Resolution.Minute).Symbol

        # ── Options ───────────────────────────────────────────────────────────
        opt = self.AddOption(TICKER, Resolution.Minute)
        # Upper expiry bound accounts for K (~30 cal days) + F (extra offset) + 20 day buffer
        _exp_max = 30 + F + 20
        opt.SetFilter(lambda u: u.Strikes(-5, 0).Expiration(0, _exp_max).PutsOnly())
        if PRICE_MODEL == "BT":
            opt.PriceModel = OptionPriceModels.BinomialCoxRossRubinstein()
        elif PRICE_MODEL == "BS":
            opt.PriceModel = OptionPriceModels.BlackScholes()
        # "default" → no assignment, QC uses built-in model
        self.option_symbol = opt.Symbol

        # ── Fetch earnings dates ──────────────────────────────────────────────
        self.earnings_dates = self._load_earnings_dates()
        self._log_earnings_dates()

        self.traded_earnings = set()
        self._trade_log      = []          # list of dicts, one per closed trade

        # ── Position state ────────────────────────────────────────────────────
        self.state            = "FLAT"
        self.put_symbol       = None
        self.put_contracts    = 0
        self.put_entry_price  = 0.0
        self.put_entry_iv     = 0.0
        self.stock_qty        = 0
        self.stock_cost_basis = 0.0
        self.stock_realized   = 0.0
        self.last_hedge_price = 0.0
        self.entry_earnings   = None
        self.entry_date       = None

        # ── Option chain cache ────────────────────────────────────────────────
        self._chain = None

        # ── Dynamic entry scan state ──────────────────────────────────────────
        self._iv_samples    = []     # IV readings collected during baseline window
        self._iv_avg        = None   # arithmetic mean of first 6 samples
        self._scan_earnings = None   # Earnings date currently being scanned

        # ── Realized-vol at entry ──────────────────────────────────────────────
        self.entry_rv = 0.0          # 30-cal-day RV computed when put is bought

        # ── Scheduled events ──────────────────────────────────────────────────
        self.Schedule.On(
            self.DateRules.EveryDay(TICKER),
            self.TimeRules.AfterMarketOpen(TICKER, 5),
            self._manage_position,
        )
        self.Schedule.On(
            self.DateRules.EveryDay(TICKER),
            self.TimeRules.BeforeMarketClose(TICKER, 30),
            self._delta_hedge,
        )

    # ─── Earnings date loading ────────────────────────────────────────────────

    def _load_earnings_dates(self):
        try:
            start = Date(self.StartDate.year, self.StartDate.month, self.StartDate.day)
        except AttributeError:
            start = Date(self.StartDate.Year, self.StartDate.Month, self.StartDate.Day)
        try:
            end = Date(self.EndDate.year, self.EndDate.month, self.EndDate.day)
        except AttributeError:
            end = Date(self.EndDate.Year, self.EndDate.Month, self.EndDate.Day)

        combined = []

        if MANUAL_EARNINGS_DATES:
            manual = [d for d in MANUAL_EARNINGS_DATES if start <= d.date() <= end]
            combined.extend(manual)

        if FMP_API_KEY:
            try:
                fmp_dates = _fetch_earnings_fmp(TICKER, N, FMP_API_KEY, start, end)
                combined.extend(fmp_dates)
            except Exception:
                pass

        combined = sorted(set(combined))
        return combined[-N:] if N and len(combined) > N else combined

    # ─── Mid-price fill model ─────────────────────────────────────────────────

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFillModel(MidPriceFillModel())

    # ─── Data handler ─────────────────────────────────────────────────────────

    def OnData(self, data):
        if self.option_symbol in data.OptionChains:
            self._chain = data.OptionChains[self.option_symbol]

    # ─── Position management (5 min after open) ───────────────────────────────

    def _manage_position(self):
        if not self.earnings_dates:
            return

        today = self.Time.date()

        for ed_dt in reversed(self.earnings_dates):
            ed = ed_dt.date() if isinstance(ed_dt, datetime) else ed_dt
            if ed in self.traded_earnings:
                continue

            exit_day  = self._offset_trading_days(ed, -1)
            entry_day = self._offset_trading_days(ed, -K)

            if today == exit_day and self.state == "ACTIVE":
                if self.entry_earnings and self.entry_earnings.date() == ed:
                    self._exit_position()
                    break

            if self.state == "FLAT" and not DYNAMIC_ENTRY:
                # Original behaviour: enter on exactly day K
                if today == entry_day:
                    self._enter_position(ed_dt)
                    break
            # DYNAMIC_ENTRY scanning happens at EOD in _delta_hedge
            # where Greeks (Vega, Theta) are reliably populated.

    # ─── Dynamic scan entry ───────────────────────────────────────────────────

    def _scan_entry(self, earnings_dt):
        """
        DYNAMIC_ENTRY logic:
          Phase 1 — Build baseline: collect ATM-put IV at 3:30 PM each day,
                    starting day K.  Stop after 6 valid readings.
          Phase 2 — Entry trigger: enter when current_IV / iv_avg >= W.
        """
        ed = earnings_dt.date() if isinstance(earnings_dt, datetime) else earnings_dt

        if self._chain is None:
            return

        stock   = self.Securities[self.stock_symbol]
        s_price = stock.Price
        if s_price <= 0:
            return

        put = self._select_put(self._chain, ed, s_price)
        if put is None:
            return

        try:
            current_iv = put.ImpliedVolatility
        except Exception:
            current_iv = 0.0

        if current_iv <= 0:
            return

        # Reset state when we start tracking a new earnings event
        if self._scan_earnings != ed:
            self._iv_samples    = []
            self._iv_avg        = None
            self._scan_earnings = ed

        # Phase 1: build the 6-day baseline average
        if self._iv_avg is None:
            self._iv_samples.append(current_iv)
            if len(self._iv_samples) >= 5:
                self._iv_avg = sum(self._iv_samples) / len(self._iv_samples)
            return   # never enter during the baseline window

        # Phase 2: fire when IV has risen W× above the baseline average
        if current_iv / self._iv_avg >= W:
            self._enter_position(earnings_dt)

    # ─── Entry ────────────────────────────────────────────────────────────────

    def _enter_position(self, earnings_dt):
        ed = earnings_dt.date() if isinstance(earnings_dt, datetime) else earnings_dt

        if self._chain is None:
            return

        stock   = self.Securities[self.stock_symbol]
        s_price = stock.Price
        if s_price <= 0:
            return

        put = self._select_put(self._chain, ed, s_price)
        if put is None:
            return

        put_mid = _mid(put.BidPrice, put.AskPrice)
        if put_mid <= 0:
            return

        n_contracts = max(1, int(S / (put_mid * 100)))

        # ── Realized volatility (30 cal-day lookback) + IV/RV filter ──────────
        rv = 0.0
        try:
            hist = self.History(self.stock_symbol, timedelta(days=30), Resolution.Daily)
            if hist is not None and not hist.empty:
                closes = hist['close'].tolist()
                if len(closes) >= 2:
                    log_rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
                    mean     = sum(log_rets) / len(log_rets)
                    var      = sum((r - mean) ** 2 for r in log_rets) / max(len(log_rets) - 1, 1)
                    rv       = (var ** 0.5) * (252 ** 0.5)
        except Exception:
            rv = 0.0

        try:
            cur_iv = put.ImpliedVolatility
        except Exception:
            cur_iv = 0.0

        if Z > 0 and rv > 0:
            if cur_iv / rv >= Z:
                return

        self.MarketOrder(put.Symbol, n_contracts)

        delta    = put.Greeks.Delta
        n_shares = max(0, round(abs(n_contracts * 100 * delta)))
        s_mid    = _mid(stock.BidPrice, stock.AskPrice)
        if n_shares > 0:
            self.MarketOrder(self.stock_symbol, n_shares)

        self.state            = "ACTIVE"
        self.put_symbol       = put.Symbol
        self.put_contracts    = n_contracts
        self.put_entry_price  = put_mid
        self.put_entry_iv     = cur_iv
        self.stock_qty        = n_shares
        self.stock_cost_basis = n_shares * s_mid
        self.stock_realized   = 0.0
        self.last_hedge_price = s_price
        self.entry_earnings   = earnings_dt
        self.entry_rv         = rv
        self.entry_date       = self.Time.date()

        pass  # entry logged in OnEndOfAlgorithm summary only

    # ─── Exit ─────────────────────────────────────────────────────────────────

    def _exit_position(self):
        if self.state != "ACTIVE":
            return

        stock = self.Securities[self.stock_symbol]
        s_mid = _mid(stock.BidPrice, stock.AskPrice)

        put_mid     = 0.0
        put_exit_iv = 0.0
        if self._chain:
            for c in self._chain:
                if c.Symbol == self.put_symbol:
                    put_mid = _mid(c.BidPrice, c.AskPrice)
                    try:
                        put_exit_iv = c.ImpliedVolatility
                    except Exception:
                        put_exit_iv = 0.0
                    break

        if put_mid == 0.0:
            try:
                ps      = self.Securities[self.put_symbol]
                put_mid = _mid(ps.BidPrice, ps.AskPrice)
            except Exception:
                pass

        if self.put_contracts > 0:
            self.MarketOrder(self.put_symbol, -self.put_contracts)

        if self.stock_qty > 0:
            self.MarketOrder(self.stock_symbol, -self.stock_qty)

        put_pnl   = (put_mid - self.put_entry_price) * self.put_contracts * 100
        stk_unr   = self.stock_qty * s_mid - self.stock_cost_basis
        stk_pnl   = self.stock_realized + stk_unr
        total_pnl = put_pnl + stk_pnl

        ed = self.entry_earnings.date()

        self._trade_log.append({
            "earnings":   ed,
            "put_pnl":    put_pnl,
            "stk_pnl":    stk_pnl,
            "total":      total_pnl,
            "iv_entry":   self.put_entry_iv,
            "iv_exit":    put_exit_iv,
            "rv":         self.entry_rv,
            "entry_date": self.entry_date,
            "exit_date":  self.Time.date(),
            "iv_avg":     self._iv_avg,
        })
        self.traded_earnings.add(ed)
        self._reset()

    # ─── Delta hedge (30 min before close) ────────────────────────────────────

    def _delta_hedge(self):
        # ── DYNAMIC_ENTRY: scan for entry at EOD when Greeks are reliable ─────
        if DYNAMIC_ENTRY and self.state == "FLAT" and self._chain is not None:
            today = self.Time.date()
            for ed_dt in reversed(self.earnings_dates):
                ed = ed_dt.date() if isinstance(ed_dt, datetime) else ed_dt
                if ed in self.traded_earnings:
                    continue
                exit_day  = self._offset_trading_days(ed, -1)
                entry_day = self._offset_trading_days(ed, -K)
                if entry_day <= today <= exit_day:
                    self._scan_entry(ed_dt)
                    break
        # ─────────────────────────────────────────────────────────────────────

        if self.state != "ACTIVE" or self._chain is None:
            return

        stock   = self.Securities[self.stock_symbol]
        s_price = stock.Price

        if self.last_hedge_price <= 0:
            return

        # ── Dynamic hedge band from entry IV ──────────────────────────────
        # Uses IV at the time the position was entered (stable throughout hold).
        # daily_sigma = annualised IV / sqrt(252), converted to %.
        daily_sigma = self.put_entry_iv / (252 ** 0.5) * 100
        dynamic_d   = max(D_floor, min(D_ceil, D_mult * daily_sigma))

        pct_move = (s_price / self.last_hedge_price - 1) * 100
        if abs(pct_move) < dynamic_d:
            return

        cur_delta   = None
        cur_iv      = 0.0
        cur_put_mid = 0.0
        for c in self._chain:
            if c.Symbol == self.put_symbol:
                cur_delta   = c.Greeks.Delta
                try:
                    cur_iv  = c.ImpliedVolatility
                except Exception:
                    cur_iv  = 0.0
                cur_put_mid = _mid(c.BidPrice, c.AskPrice)
                break

        if cur_delta is None:
            return

        if cur_delta == 0.0 and cur_iv == 0.0:
            return

        target = max(0, round(abs(self.put_contracts * 100 * cur_delta)))
        adj    = target - self.stock_qty
        if adj == 0:
            return

        s_mid = _mid(stock.BidPrice, stock.AskPrice)
        self.MarketOrder(self.stock_symbol, adj)

        if adj > 0:
            self.stock_cost_basis += adj * s_mid
        else:
            sold     = abs(adj)
            avg_cost = self.stock_cost_basis / self.stock_qty if self.stock_qty else s_mid
            self.stock_realized   += (s_mid - avg_cost) * sold
            self.stock_cost_basis -= avg_cost * sold

        self.stock_qty        = target
        self.last_hedge_price = s_price

        put_pnl   = (cur_put_mid - self.put_entry_price) * self.put_contracts * 100
        stk_unr   = self.stock_qty * s_mid - self.stock_cost_basis
        stk_pnl   = self.stock_realized + stk_unr
        total_pnl = put_pnl + stk_pnl

        pass  # hedge details suppressed — summary in OnEndOfAlgorithm

    # ─── Helpers ──────────────────────────────────────────────────────────────

    def _select_put(self, chain, earnings_date, stock_price):
        """Select ATM or slightly OTM put expiring at least F days after earnings."""
        puts = [c for c in chain if c.Right == OptionRight.Put]
        if not puts:
            return None

        min_expiry = earnings_date + timedelta(days=F)
        after = [p for p in puts if p.Expiry.date() >= min_expiry]
        if not after:
            return None
        pool = after

        closest_expiry = min(p.Expiry for p in pool)
        by_expiry      = [p for p in pool if p.Expiry == closest_expiry]

        atm_or_otm = [p for p in by_expiry if p.Strike <= stock_price]
        if not atm_or_otm:
            atm_or_otm = sorted(by_expiry, key=lambda x: x.Strike)

        return max(atm_or_otm, key=lambda x: x.Strike)

    def _offset_trading_days(self, ref_date, offset_days):
        """Return the date offset_days trading days from ref_date."""
        d    = ref_date if isinstance(ref_date, Date) else ref_date.date()
        step = -1 if offset_days < 0 else 1
        left = abs(offset_days)
        exch = self.Securities[self.stock_symbol].Exchange
        while left > 0:
            d += timedelta(days=step)
            if exch.Hours.IsDateOpen(d):
                left -= 1
        return d

    def _log_earnings_dates(self):
        pass  # suppressed — summary printed in OnEndOfAlgorithm

    def _reset(self):
        self.state             = "FLAT"
        self.put_symbol        = None
        self.put_contracts     = 0
        self.put_entry_price   = 0.0
        self.put_entry_iv      = 0.0
        self.stock_qty         = 0
        self.stock_cost_basis  = 0.0
        self.stock_realized    = 0.0
        self.last_hedge_price  = 0.0
        self.entry_earnings    = None
        self.entry_date        = None
        self._iv_samples    = []
        self._iv_avg        = None
        self._scan_earnings = None
        self.entry_rv       = 0.0


    # ─── End-of-backtest summary ──────────────────────────────────────────────

    def OnEndOfAlgorithm(self):
        n = len(self._trade_log)
        if n == 0:
            self.Log("[SUMMARY] No trades completed.")
            return

        totals = {"put": 0.0, "stk": 0.0, "total": 0.0}
        wins   = sum(1 for t in self._trade_log if t["total"] >= 0)

        self.Log(f"{'─'*72}")
        self.Log(f"  {TICKER} BACKTEST SUMMARY  |  {n} trade(s)  |  Wins: {wins}/{n}")
        self.Log(f"{'─'*72}")
        self.Log(f"  {'Earnings':<12} {'Put PnL':>12} {'Stock PnL':>12} {'Combined':>12}   IV entry → exit  IV chg    IV/RV")
        self.Log(f"  {'─'*88}")
        for t in self._trade_log:
            tag      = "[+]" if t["total"] >= 0 else "[-]"
            rv       = t.get("rv", 0.0)
            ratio    = f"{t['iv_entry'] / rv:.2f}x" if rv > 0 else "n/a"
            iv_chg   = (t['iv_exit'] - t['iv_entry']) / t['iv_entry'] * 100 if t['iv_entry'] > 0 else 0.0
            days_held = (t["exit_date"] - t["entry_date"]).days if t.get("entry_date") and t.get("exit_date") else 0
            iv_avg    = t.get("iv_avg")
            iv_avg_str = f"avg={iv_avg:.1%}" if iv_avg else "avg=n/a"
            self.Log(
                f"  {tag} {t['earnings']!s:<11}"
                f"  ${t['put_pnl']:>+10,.2f}"
                f"  ${t['stk_pnl']:>+10,.2f}"
                f"  ${t['total']:>+10,.2f}"
                f"  [{iv_avg_str}]"
                f"   {t['iv_entry']:.1%} → {t['iv_exit']:.1%}"
                f"  {iv_chg:>+6.0f}%"
                f"    {ratio}"
                f"  DAYS-{days_held}"
            )
            totals["put"]   += t["put_pnl"]
            totals["stk"]   += t["stk_pnl"]
            totals["total"] += t["total"]

        avg = totals["total"] / n
        self.Log(f"  {'─'*88}")
        self.Log(f"  {'TOTAL':<15}  ${totals['put']:>+10,.2f}  ${totals['stk']:>+10,.2f}  ${totals['total']:>+10,.2f}")
        self.Log(f"  Avg PnL/trade: ${avg:+,.2f}")
        self.Log(f"{'─'*72}")


# ─── Mid-price fill model ─────────────────────────────────────────────────────

class MidPriceFillModel(ImmediateFillModel):
    def MarketFill(self, asset, order):
        fill = ImmediateFillModel.MarketFill(self, asset, order)
        mid  = _mid(asset.BidPrice, asset.AskPrice)
        if mid > 0:
            fill.FillPrice = round(mid, 2)
        return fill


# ─── Utility ──────────────────────────────────────────────────────────────────

def _mid(bid, ask):
    if bid > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return 0.0
