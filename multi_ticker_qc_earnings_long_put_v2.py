# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import requests
import math
# endregion

# ─── Configurable Parameters ──────────────────────────────────────────────────

N      = 16       # Number of past earnings events per ticker (most recent N)
K      = 14       # Fixed entry day (trading days before earnings)
                  # Also used as scan start when DYNAMIC_ENTRY = True
DYNAMIC_ENTRY = False  # False → enter at exactly K days (original behaviour)
                       # True  → scan from K days, enter on vega/theta signal
M      = 5.0      # (DYNAMIC_ENTRY only — legacy, unused with new IV-ratio trigger)
W      = 1.15     # (DYNAMIC_ENTRY only) enter when current_IV / 6-day-avg_IV >= W
                  # e.g. 1.15 → enter when IV has risen 15% above its baseline average
S      = 10_000   # Notional USD value of puts to buy at entry (per ticker)
D_mult  = 1.0    # Delta-tolerance scalar: tolerance = D_mult × daily_sigma_frac × |option_exposure|
                  # e.g. 1.0 → tolerate up to 1 daily-sigma of delta drift before re-hedging
RV_SIGMA = True   # True  → hedge tolerance sigma from live 30-day realized vol (refreshed daily)
                  # False → hedge tolerance sigma from put IV at entry (fixed for life of trade)
F      = 0        # Minimum calendar days after earnings date for put expiry
Z      = 0.0      # IV/RV filter: skip entry if IV/RV >= Z  (0.0 = disabled)
PRICE_MODEL = "BT"   # Option pricing model for Greeks: "BT" | "BS" | "default"
                     # BT  = Binomial CoxRossRubinstein (American equity options — recommended)
                     # BS  = Black-Scholes (European-style, faster, ignores early exercise)
                     # default = QC built-in (no explicit model set)

# ─── Financial Modeling Prep API ──────────────────────────────────────────────
FMP_API_KEY = ""   # Leave empty to rely solely on MANUAL_EARNINGS_DATES below

# ─── Earnings Dates ───────────────────────────────────────────────────────────
# Add one key per ticker; the strategy runs on every ticker in this dict.
# Format: datetime(YYYY, M, D)  — use the announcement date
#
# Example:
#   MANUAL_EARNINGS_DATES = {
#       "AAPL": [ datetime(2024, 2, 1), datetime(2024, 5, 2), ... ],
#       "NVDA": [ datetime(2024, 2, 21), datetime(2024, 5, 22), ... ],
#   }

MANUAL_EARNINGS_DATES = {
    "AAPL": [
        datetime(2022, 1, 27),   # Q1 FY2022
        datetime(2022, 4, 28),   # Q2 FY2022
        datetime(2022, 7, 28),   # Q3 FY2022
        datetime(2022, 10, 27),  # Q4 FY2022
        datetime(2023, 2, 2),    # Q1 FY2023
        datetime(2023, 5, 4),    # Q2 FY2023
        datetime(2023, 8, 3),    # Q3 FY2023
        datetime(2023, 11, 2),   # Q4 FY2023
        datetime(2024, 2, 1),    # Q1 FY2024
        datetime(2024, 5, 2),    # Q2 FY2024
        datetime(2024, 8, 1),    # Q3 FY2024
        datetime(2024, 10, 31),  # Q4 FY2024
        datetime(2025, 1, 30),   # Q1 FY2025
        datetime(2025, 5, 1),    # Q2 FY2025
        datetime(2025, 7, 31),   # Q3 FY2025
        datetime(2026, 1, 29),   # Q4 FY2025
    ],
}

# ──────────────────────────────────────────────────────────────────────────────

def _fetch_earnings_fmp(ticker: str, n: int, api_key: str, start_date: Date, end_date: Date):
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


def _mid(bid, ask):
    if bid > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return 0.0


class EarningsLongPutMultiTickerV2(QCAlgorithm):
    """
    Multi-ticker Earnings Long-Put + Delta-Neutral Strategy  (V2 hedge)
    ====================================================================
    Identical to V1 except for the delta-hedge trigger logic:

    V1 (original):  re-hedge when stock % move from last hedge exceeds a
                    vol-scaled band (price-move trigger).
    V2 (this file): re-hedge when the net position delta (stock + options)
                    exceeds a vol-scaled tolerance in share-equivalent terms
                    (absolute-delta trigger).

    Hedge decision each day (30 min before close):
      1. position_delta = stock_shares + (put_delta × contracts × 100)
      2. daily_sigma_frac =
           RV_SIGMA=True  → live 30-day realized vol / sqrt(252)
           RV_SIGMA=False → entry IV / sqrt(252)
      3. tolerance = D_mult × daily_sigma_frac × |option_exposure_in_shares|
      4. If |position_delta| > tolerance → hedge to delta-neutral
         Otherwise → do nothing.
    """

    # ── Initialise ────────────────────────────────────────────────────────────

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(1_000_000)

        tickers = list(MANUAL_EARNINGS_DATES.keys())

        # Per-ticker state dict
        self._ts = {}
        self._max_concurrent = 0   # peak number of tickers held simultaneously

        _exp_max = 30 + F + 20   # option chain expiry window

        for ticker in tickers:
            eq  = self.AddEquity(ticker, Resolution.Minute)
            opt = self.AddOption(ticker, Resolution.Minute)
            opt.SetFilter(lambda u: u.Strikes(-5, 0).Expiration(0, _exp_max).PutsOnly())
            if PRICE_MODEL == "BT":
                opt.PriceModel = OptionPriceModels.BinomialCoxRossRubinstein()
            elif PRICE_MODEL == "BS":
                opt.PriceModel = OptionPriceModels.BlackScholes()

            dates = self._load_earnings_dates(ticker)
            self._log_earnings_dates(ticker, dates)

            self._ts[ticker] = {
                # symbols
                "stock_symbol":  eq.Symbol,
                "option_symbol": opt.Symbol,
                # position state
                "state":           "FLAT",
                "put_symbol":      None,
                "put_contracts":   0,
                "put_entry_price": 0.0,
                "put_entry_iv":    0.0,
                "stock_qty":       0,
                "stock_cost_basis": 0.0,
                "stock_realized":  0.0,
                "last_hedge_price": 0.0,
                "entry_earnings":  None,
                "entry_rv":        0.0,
                # chain cache
                "chain": None,
                # dynamic entry scan state
                "iv_samples":    [],
                "iv_avg":        None,
                "scan_earnings": None,
                # data
                "earnings_dates":  dates,
                "traded_earnings": set(),
                "trade_log":       [],
            }

            # Scheduled events — capture ticker in default arg
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.AfterMarketOpen(ticker, 5),
                lambda t=ticker: self._manage_position(t),
            )
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.BeforeMarketClose(ticker, 30),
                lambda t=ticker: self._delta_hedge(t),
            )

    # ── Earnings date loading ─────────────────────────────────────────────────

    def _load_earnings_dates(self, ticker):
        try:
            start = Date(self.StartDate.year,  self.StartDate.month,  self.StartDate.day)
        except AttributeError:
            start = Date(self.StartDate.Year,  self.StartDate.Month,  self.StartDate.Day)
        try:
            end   = Date(self.EndDate.year,    self.EndDate.month,    self.EndDate.day)
        except AttributeError:
            end   = Date(self.EndDate.Year,    self.EndDate.Month,    self.EndDate.Day)

        combined = []
        manual   = MANUAL_EARNINGS_DATES.get(ticker, [])
        if manual:
            in_window = [d for d in manual if start <= d.date() <= end]
            combined.extend(in_window)

        if FMP_API_KEY:
            try:
                fmp_dates = _fetch_earnings_fmp(ticker, N, FMP_API_KEY, start, end)
                combined.extend(fmp_dates)
            except Exception:
                pass

        combined = sorted(set(combined))
        return combined[-N:] if N and len(combined) > N else combined

    def _log_earnings_dates(self, ticker, dates):
        pass  # suppressed — summary printed in OnEndOfAlgorithm

    # ── Fill model ────────────────────────────────────────────────────────────

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFillModel(MidPriceFillModel())

    # ── Data handler ──────────────────────────────────────────────────────────

    def OnData(self, data):
        for ticker, ts in self._ts.items():
            if ts["option_symbol"] in data.OptionChains:
                ts["chain"] = data.OptionChains[ts["option_symbol"]]

    # ── Position management (5 min after open) ────────────────────────────────

    def _manage_position(self, ticker):
        ts = self._ts[ticker]
        if not ts["earnings_dates"]:
            return

        today = self.Time.date()

        for ed_dt in reversed(ts["earnings_dates"]):
            ed = ed_dt.date() if isinstance(ed_dt, datetime) else ed_dt
            if ed in ts["traded_earnings"]:
                continue

            exit_day  = self._offset_trading_days(ticker, ed, -1)
            entry_day = self._offset_trading_days(ticker, ed, -K)

            if today == exit_day and ts["state"] == "ACTIVE":
                if ts["entry_earnings"] and ts["entry_earnings"].date() == ed:
                    self._exit_position(ticker)
                    break

            if ts["state"] == "FLAT" and not DYNAMIC_ENTRY:
                if today == entry_day:
                    self._enter_position(ticker, ed_dt)
                    break

    # ── Dynamic scan entry ────────────────────────────────────────────────────

    def _scan_entry(self, ticker, earnings_dt):
        """
        DYNAMIC_ENTRY logic:
          Phase 1 — Build baseline: collect ATM-put IV at 3:30 PM each day,
                    starting day K.  Stop after 6 valid readings.
          Phase 2 — Entry trigger: enter when current_IV / iv_avg >= W.
        """
        ts  = self._ts[ticker]
        ed  = earnings_dt.date() if isinstance(earnings_dt, datetime) else earnings_dt

        if ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        put = self._select_put(ts["chain"], ed, s_price)
        if put is None:
            return

        try:
            current_iv = put.ImpliedVolatility
        except Exception:
            current_iv = 0.0

        if current_iv <= 0:
            return

        if ts["scan_earnings"] != ed:
            ts["iv_samples"]    = []
            ts["iv_avg"]        = None
            ts["scan_earnings"] = ed

        if ts["iv_avg"] is None:
            ts["iv_samples"].append(current_iv)
            if len(ts["iv_samples"]) >= 5:
                ts["iv_avg"] = sum(ts["iv_samples"]) / len(ts["iv_samples"])
            return

        if current_iv / ts["iv_avg"] >= W:
            self._enter_position(ticker, earnings_dt)

    # ── Entry ─────────────────────────────────────────────────────────────────

    def _enter_position(self, ticker, earnings_dt):
        ts  = self._ts[ticker]
        ed  = earnings_dt.date() if isinstance(earnings_dt, datetime) else earnings_dt

        if ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        put = self._select_put(ts["chain"], ed, s_price)
        if put is None:
            return

        put_mid = _mid(put.BidPrice, put.AskPrice)
        if put_mid <= 0:
            return

        n_contracts = max(1, int(S / (put_mid * 100)))

        # ── Realized volatility + IV/RV filter ───────────────────────────────
        rv = self._calc_realized_vol(ts["stock_symbol"], 30)

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
            self.MarketOrder(ts["stock_symbol"], n_shares)

        ts["state"]            = "ACTIVE"
        ts["put_symbol"]       = put.Symbol
        ts["put_contracts"]    = n_contracts
        ts["put_entry_price"]  = put_mid
        ts["put_entry_iv"]     = cur_iv
        ts["stock_qty"]        = n_shares
        ts["stock_cost_basis"] = n_shares * s_mid
        ts["stock_realized"]   = 0.0
        ts["last_hedge_price"] = s_price
        ts["entry_earnings"]   = earnings_dt
        ts["entry_rv"]         = rv

        # Track peak concurrent positions
        active = sum(1 for t in self._ts.values() if t["state"] == "ACTIVE")
        if active > self._max_concurrent:
            self._max_concurrent = active

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _exit_position(self, ticker):
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE":
            return

        stock = self.Securities[ts["stock_symbol"]]
        s_mid = _mid(stock.BidPrice, stock.AskPrice)

        put_mid     = 0.0
        put_exit_iv = 0.0
        if ts["chain"]:
            for c in ts["chain"]:
                if c.Symbol == ts["put_symbol"]:
                    put_mid = _mid(c.BidPrice, c.AskPrice)
                    try:
                        put_exit_iv = c.ImpliedVolatility
                    except Exception:
                        put_exit_iv = 0.0
                    break

        if put_mid == 0.0:
            try:
                ps      = self.Securities[ts["put_symbol"]]
                put_mid = _mid(ps.BidPrice, ps.AskPrice)
            except Exception:
                pass

        if ts["put_contracts"] > 0:
            self.MarketOrder(ts["put_symbol"], -ts["put_contracts"])
        if ts["stock_qty"] > 0:
            self.MarketOrder(ts["stock_symbol"], -ts["stock_qty"])

        put_pnl   = (put_mid - ts["put_entry_price"]) * ts["put_contracts"] * 100
        stk_unr   = ts["stock_qty"] * s_mid - ts["stock_cost_basis"]
        stk_pnl   = ts["stock_realized"] + stk_unr
        total_pnl = put_pnl + stk_pnl

        ed = ts["entry_earnings"].date()

        ts["trade_log"].append({
            "earnings": ed,
            "put_pnl":  put_pnl,
            "stk_pnl":  stk_pnl,
            "total":    total_pnl,
            "iv_entry": ts["put_entry_iv"],
            "iv_exit":  put_exit_iv,
            "rv":       ts["entry_rv"],
        })
        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    # ── Delta hedge V2: absolute-delta tolerance (30 min before close) ────────

    def _delta_hedge(self, ticker):
        ts = self._ts[ticker]

        # DYNAMIC_ENTRY: scan for entry at EOD when Greeks are reliable
        if DYNAMIC_ENTRY and ts["state"] == "FLAT" and ts["chain"] is not None:
            today = self.Time.date()
            for ed_dt in reversed(ts["earnings_dates"]):
                ed = ed_dt.date() if isinstance(ed_dt, datetime) else ed_dt
                if ed in ts["traded_earnings"]:
                    continue
                exit_day  = self._offset_trading_days(ticker, ed, -1)
                entry_day = self._offset_trading_days(ticker, ed, -K)
                if entry_day <= today <= exit_day:
                    self._scan_entry(ticker, ed_dt)
                    break

        if ts["state"] != "ACTIVE" or ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        # ── Step 1: Compute live position delta ──────────────────────────────
        # Find the current put Greeks from the live chain
        cur_delta   = None
        cur_iv      = 0.0
        cur_put_mid = 0.0
        for c in ts["chain"]:
            if c.Symbol == ts["put_symbol"]:
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

        # Stock delta = number of shares (each share has delta +1)
        stock_delta = ts["stock_qty"]

        # Option delta contribution = put_delta × contracts × 100
        # (put_delta is negative, so this is negative for long puts)
        option_delta = cur_delta * ts["put_contracts"] * 100

        # Net position delta (should be near zero if well-hedged)
        position_delta = stock_delta + option_delta

        # ── Step 2: Daily sigma fraction ─────────────────────────────────────
        if RV_SIGMA:
            # Use live 30-day realized vol (refreshed each hedge check)
            rv = self._calc_realized_vol(ts["stock_symbol"], 30)
            if rv <= 0:
                return
            daily_sigma_frac = rv / (252 ** 0.5)
        else:
            # Use the put's IV captured at entry (fixed for life of trade)
            entry_iv = ts["put_entry_iv"]
            if entry_iv <= 0:
                return
            daily_sigma_frac = entry_iv / (252 ** 0.5)

        # ── Step 3: Tolerance (in shares) ────────────────────────────────────
        # tolerance = D_mult × IV × sqrt(1/252) × |option_exposure_in_shares|
        option_exposure = abs(ts["put_contracts"] * 100)
        tolerance = D_mult * daily_sigma_frac * option_exposure

        # ── Step 4: Hedge decision ───────────────────────────────────────────
        if abs(position_delta) <= tolerance:
            return   # within tolerance — do nothing

        # Re-hedge to delta-neutral: target stock qty offsets option delta
        target = max(0, round(abs(option_delta)))
        adj    = target - ts["stock_qty"]
        if adj == 0:
            return

        s_mid = _mid(stock.BidPrice, stock.AskPrice)
        self.MarketOrder(ts["stock_symbol"], adj)

        if adj > 0:
            ts["stock_cost_basis"] += adj * s_mid
        else:
            sold     = abs(adj)
            avg_cost = ts["stock_cost_basis"] / ts["stock_qty"] if ts["stock_qty"] else s_mid
            ts["stock_realized"]   += (s_mid - avg_cost) * sold
            ts["stock_cost_basis"] -= avg_cost * sold

        ts["stock_qty"]        = target
        ts["last_hedge_price"] = s_price   # kept for reference / logging

        put_pnl   = (cur_put_mid - ts["put_entry_price"]) * ts["put_contracts"] * 100
        stk_unr   = ts["stock_qty"] * s_mid - ts["stock_cost_basis"]
        stk_pnl   = ts["stock_realized"] + stk_unr
        total_pnl = put_pnl + stk_pnl

        pass  # hedge details suppressed — summary in OnEndOfAlgorithm

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _select_put(self, chain, earnings_date, stock_price):
        """Select ATM or slightly OTM put expiring at least F days after earnings."""
        puts = [c for c in chain if c.Right == OptionRight.Put]
        if not puts:
            return None
        min_expiry = earnings_date + timedelta(days=F)
        after = [p for p in puts if p.Expiry.date() >= min_expiry]
        if not after:
            return None
        closest_expiry = min(p.Expiry for p in after)
        by_expiry      = [p for p in after if p.Expiry == closest_expiry]
        atm_or_otm     = [p for p in by_expiry if p.Strike <= stock_price]
        if not atm_or_otm:
            atm_or_otm = sorted(by_expiry, key=lambda x: x.Strike)
        return max(atm_or_otm, key=lambda x: x.Strike)

    def _offset_trading_days(self, ticker, ref_date, offset_days):
        """Return the date offset_days trading days from ref_date."""
        d    = ref_date if isinstance(ref_date, Date) else ref_date.date()
        step = -1 if offset_days < 0 else 1
        left = abs(offset_days)
        exch = self.Securities[self._ts[ticker]["stock_symbol"]].Exchange
        while left > 0:
            d += timedelta(days=step)
            if exch.Hours.IsDateOpen(d):
                left -= 1
        return d

    def _calc_realized_vol(self, symbol, lookback_days=30):
        """Compute annualized realized volatility from daily log returns.

        Args:
            symbol:        Equity symbol to fetch history for.
            lookback_days: Calendar days of history to request (default 30).

        Returns:
            Annualized realized vol (float), or 0.0 on failure / insufficient data.
        """
        try:
            hist = self.History(symbol, timedelta(days=lookback_days), Resolution.Daily)
            if hist is None or hist.empty:
                return 0.0
            closes = hist['close'].tolist()
            if len(closes) < 2:
                return 0.0
            log_rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
            mean = sum(log_rets) / len(log_rets)
            var  = sum((r - mean) ** 2 for r in log_rets) / max(len(log_rets) - 1, 1)
            return (var ** 0.5) * (252 ** 0.5)
        except Exception:
            return 0.0

    def _reset(self, ticker):
        ts = self._ts[ticker]
        ts["state"]            = "FLAT"
        ts["put_symbol"]       = None
        ts["put_contracts"]    = 0
        ts["put_entry_price"]  = 0.0
        ts["put_entry_iv"]     = 0.0
        ts["stock_qty"]        = 0
        ts["stock_cost_basis"] = 0.0
        ts["stock_realized"]   = 0.0
        ts["last_hedge_price"] = 0.0
        ts["entry_earnings"]   = None
        ts["iv_samples"]       = []
        ts["iv_avg"]           = None
        ts["scan_earnings"]    = None
        ts["entry_rv"]         = 0.0

    # ── End-of-backtest summary ───────────────────────────────────────────────

    def OnEndOfAlgorithm(self):
        grand_total = 0.0

        grand_trades = 0
        grand_wins   = 0

        for ticker in self._ts:
            trade_log = self._ts[ticker]["trade_log"]
            n         = len(trade_log)

            self.Log(f"{'═'*72}")
            if n == 0:
                self.Log(f"  {ticker} SUMMARY  |  No trades completed")
                self.Log(f"{'═'*72}")
                continue

            totals = {"put": 0.0, "stk": 0.0, "total": 0.0}
            wins   = sum(1 for t in trade_log if t["total"] >= 0)
            grand_trades += n
            grand_wins   += wins

            self.Log(f"  {ticker} SUMMARY  |  {n} trade(s)  |  Wins: {wins}/{n}")
            self.Log(f"{'─'*72}")
            self.Log(f"  {'Earnings':<12} {'Put PnL':>12} {'Stock PnL':>12} {'Combined':>12}   IV entry → exit  IV chg    IV/RV")
            self.Log(f"  {'─'*88}")

            for t in trade_log:
                tag    = "[+]" if t["total"] >= 0 else "[-]"
                rv     = t.get("rv", 0.0)
                ratio  = f"{t['iv_entry'] / rv:.2f}x" if rv > 0 else "n/a"
                iv_chg = (t['iv_exit'] - t['iv_entry']) / t['iv_entry'] * 100 if t['iv_entry'] > 0 else 0.0
                self.Log(
                    f"  {tag} {t['earnings']!s:<11}"
                    f"  ${t['put_pnl']:>+10,.2f}"
                    f"  ${t['stk_pnl']:>+10,.2f}"
                    f"  ${t['total']:>+10,.2f}"
                    f"   {t['iv_entry']:.1%} → {t['iv_exit']:.1%}"
                    f"  {iv_chg:>+6.0f}%"
                    f"    {ratio}"
                )
                totals["put"]   += t["put_pnl"]
                totals["stk"]   += t["stk_pnl"]
                totals["total"] += t["total"]

            avg = totals["total"] / n
            self.Log(f"  {'─'*88}")
            self.Log(
                f"  {'TOTAL':<15}  ${totals['put']:>+10,.2f}"
                f"  ${totals['stk']:>+10,.2f}"
                f"  ${totals['total']:>+10,.2f}"
            )
            self.Log(f"  Avg PnL/trade: ${avg:+,.2f}")
            self.Log(f"{'═'*72}")
            grand_total += totals["total"]

        if len(self._ts) > 1:
            self.Log(f"{'═'*72}")
            self.Log(f"  ALL TICKERS COMBINED  |  {grand_trades} trade(s)  |  Wins: {grand_wins}/{grand_trades}  |  Max concurrent positions: {self._max_concurrent}")
            self.Log(f"  Combined PnL: ${grand_total:+,.2f}  |  Avg PnL/trade: ${grand_total / grand_trades:+,.2f}" if grand_trades else f"  Combined PnL: ${grand_total:+,.2f}")
            self.Log(f"{'═'*72}")


# ─── Mid-price fill model ─────────────────────────────────────────────────────

class MidPriceFillModel(ImmediateFillModel):
    def MarketFill(self, asset, order):
        fill = ImmediateFillModel.MarketFill(self, asset, order)
        mid  = _mid(asset.BidPrice, asset.AskPrice)
        if mid > 0:
            fill.FillPrice = round(mid, 2)
        return fill
