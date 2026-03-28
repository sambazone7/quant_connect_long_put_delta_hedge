# region imports
from AlgorithmImports import *
from QuantConnect.DataSource import *
from datetime import timedelta, date as Date
from collections import deque
import requests
import math
# endregion

# ─── Configurable Parameters ──────────────────────────────────────────────────

N      = 16       # Number of past earnings events per ticker (most recent N)
K      = 15       # Fixed entry day (trading days before earnings)
                  # Also used as scan start when DYNAMIC_ENTRY = True
DYNAMIC_ENTRY = False  # False → enter at exactly K days (original behaviour)
                       # True  → scan from K days, enter on vega/theta signal
M      = 5.0      # (DYNAMIC_ENTRY only — legacy, unused with new IV-ratio trigger)
W      = 1.15     # (DYNAMIC_ENTRY only) enter when current_IV / 6-day-avg_IV >= W
                  # e.g. 1.15 → enter when IV has risen 15% above its baseline average
S      = 10_000   # Notional USD value of puts to buy at entry (per ticker)
HEDGE_MODE = "gamma"  # "gamma" → fixed PnL-tolerance trigger (ΔS = √(2·tol/Γ))
                      # "theta" → theta-scaled PnL trigger (tol = THETA_K × |daily θ|)
                      # "sigma" → original vol-scaled delta tolerance
PNL_TOLERANCE = 100   # (gamma mode) Dollar P&L threshold per position before re-hedging
THETA_K       = 1.0   # (theta mode) Scalar on daily theta: tol = THETA_K × |θ_daily_position|
                      # 1.0 = re-hedge at the theta-gamma breakeven move
                      # <1  = tighter (hedge before breakeven)  >1 = looser
MIN_TOLERANCE = 50    # (theta mode) Floor on dynamic tolerance to guard against near-zero theta
DRIFT_FLOOR   = 0.10  # (gamma/theta mode) Max |position_delta| as fraction of option exposure
                      # Catches time-decay drift when stock is flat (0.10 = 10%)
D_mult  = 1.0    # (sigma mode) tolerance = D_mult × daily_sigma_frac × |option_exposure|
RV_SIGMA = True   # (sigma mode) True → live 30-day RV | False → put's live IV (fallback entry IV)
F      = 0        # Minimum calendar days after earnings date for put expiry
EXPIRE_SERIES = 0  # 0 → closest expiry after earnings (default)
                   # 1 → second closest weekly expiry after earnings
                   #     (falls back to closest if no weeklies available)
Z      = 0.0      # IV/RV filter: skip entry if IV/RV >= Z  (0.0 = disabled)
MAX_PUT_PCT = 0.15  # Sanity: skip entry if put_mid > stock_price × MAX_PUT_PCT
PUT_LIMIT_MULT = 1.2  # Limit order at put_mid × this (prevents bad fills)
SPREAD_CUTOFF_PCT = 0.20  # Max bid-ask spread as fraction of option mid price (0.20 = 20%)
                          # Skip entry if |bid-ask| / mid > this.  0 = disabled.
PRICE_MODEL = "default"   # Option pricing model for Greeks: "BT" | "BS" | "default"
                          # BT  = Binomial CoxRossRubinstein (American equity options — recommended)
                          # BS  = Black-Scholes (European-style, faster, ignores early exercise)
                          # default = QC built-in (no explicit model set)
HOURLY_BARS = False       # True  → Resolution.Hour  (fast, ~50x fewer data points)
                          # False → Resolution.Minute (precise fills, slower)
TRADE_TIME_MIN = 270      # Minutes after market open to enter/exit trades
                          # 270 → 2:00 PM ET,  210 → 1:00 PM ET,  330 → 3:00 PM ET
HEDGE_TIME_MIN = 15       # Minutes before market close to run delta hedge
                          # 15 → 3:45 PM ET,  30 → 3:30 PM ET
IV_LOOKBACK        = 126  # Trading days of IV history for IV Rank min/max
IV_RANK_SMOOTH_N   = 10   # Average the N lowest/highest IV samples for rank min/max
IV_RANK_TARGET_DTE = 50   # IV rank sampling: closest expiry with DTE >= this; OTM put (strike < spot)

# ─── Financial Modeling Prep API ──────────────────────────────────────────────
FMP_API_KEY = ""   # Leave empty to rely solely on MANUAL_EARNINGS_DATES below

# ─── Earnings Dates ───────────────────────────────────────────────────────────
# Imported from tickerlist.py — add one key per ticker there.
from listqqqAll import MANUAL_EARNINGS_DATES

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
    Multi-ticker Earnings Long-Put + Delta-Neutral Strategy  (V3 hedge)
    ====================================================================
    Supports three hedge trigger modes (HEDGE_MODE config):

    "gamma"  — Fixed PnL-tolerance trigger based on option gamma.
               ΔS_trigger = √(2 × PNL_TOLERANCE / Γ_position)
               Re-hedge when stock move since last hedge exceeds ΔS_trigger.
               Adapts to moneyness: hedges more near ATM, less when OTM.

    "theta"  — Theta-scaled PnL trigger (theta-gamma breakeven).
               Same ΔS formula but PNL_TOLERANCE = THETA_K × |daily_θ|.
               Adapts to both moneyness AND option lifecycle: tighter
               when theta is cheap (far from expiry), wider when theta
               is expensive (near expiry). THETA_K=1.0 = breakeven move.

    "sigma"  — Original V2 vol-scaled delta tolerance.
               Re-hedge when |position_delta| > D_mult × σ_daily × exposure.

    Both gamma and theta modes include a DRIFT_FLOOR fallback that
    catches time-decay-driven delta drift when the stock is flat.

    Hedge decision each day (HEDGE_TIME_MIN before close):
      1. position_delta = stock_shares + (put_delta × contracts × 100)
      2. Gamma/theta: ΔS_trigger from live gamma + tolerance
         Sigma: tolerance from D_mult × daily_sigma × exposure
      3. If trigger exceeded → hedge to delta-neutral
         Otherwise → do nothing.
    """

    # ── Initialise ────────────────────────────────────────────────────────────

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(20_000_000)

        self.SetWarmUp(timedelta(days=IV_LOOKBACK + 60))

        self._vix_symbol = self.AddData(CBOE, "VIX", Resolution.Daily).Symbol

        tickers = list(MANUAL_EARNINGS_DATES.keys())

        # Per-ticker state dict
        self._ts = {}
        self._max_concurrent = 0   # peak number of tickers held simultaneously
        self._filling_ticker = None  # set around our own orders to distinguish from auto-liquidation

        _exp_max = max(30 + F + 20, IV_RANK_TARGET_DTE + 30)

        for ticker in tickers:
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            eq  = self.AddEquity(ticker, _res)
            opt = self.AddOption(ticker, _res)
            opt.SetFilter(lambda u: u.Strikes(-5, 0).Expiration(0, _exp_max).PutsOnly())
            if PRICE_MODEL == "BT":
                opt.PriceModel = OptionPriceModels.BinomialCoxRossRubinstein()
            elif PRICE_MODEL == "BS":
                opt.PriceModel = OptionPriceModels.BlackScholes()
            opt.SetOptionAssignmentModel(NullAssignmentModel())

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
                "put_entry_fill":  0.0,
                "put_exit_fill":   0.0,
                "put_entry_iv":    0.0,
                "put_order_ticket": None,
                "pending_hedge_shares": 0,
                "stock_qty":       0,
                "stock_cost_basis": 0.0,
                "stock_realized":  0.0,
                "hedge_count":     0,
                "last_hedge_price": 0.0,
                "entry_earnings":  None,
                "entry_rv":        0.0,
                "iv_min":          None,
                "iv_min_date":     None,
                "iv_max":          None,
                "iv_max_date":     None,
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
                "force_exited":    False,
                "call_symbol":     None,
                "iv_early_put":    0.0,
                "put_spread_entry": 0,
                "put_spread_exit": 0,
                "call_spread_exit": 0,
                # IV rank state
                "iv_history":          deque(maxlen=IV_LOOKBACK),
                "last_iv_sample_date": None,
                "iv_sample_fails":        0,
                "iv_sample_fails_warmup": 0,
                "entry_iv_rank":       None,
                "exit_iv_rank":        None,
                "iv_enter_sample":     None,
                "iv_exit_sample":      None,
                "entry_iv_pctl":       None,
                "exit_iv_pctl":        None,
                "vix_entry":           None,
                "vix_exit":            None,
            }

            # Scheduled events — capture ticker in default arg
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.AfterMarketOpen(ticker, TRADE_TIME_MIN),
                lambda t=ticker: self._manage_position(t),
            )
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.BeforeMarketClose(ticker, HEDGE_TIME_MIN),
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

            # ── Stock split detection ───────────────────────────────────
            if data.Splits.ContainsKey(ts["stock_symbol"]):
                split = data.Splits[ts["stock_symbol"]]
                if split.Type == SplitType.SplitOccurred:
                    # Always adjust qty so avg_cost stays correct post-split
                    # (cost_basis unchanged ÷ fewer shares = correct post-split avg)
                    if ts["state"] in ("ACTIVE", "EMERGENCY_PENDING"):
                        if ts["stock_qty"] > 0 and split.SplitFactor > 0:
                            ts["stock_qty"] = round(ts["stock_qty"] / split.SplitFactor)
                    # Only trigger emergency exit if not already pending
                    if ts["state"] == "ACTIVE" and not ts.get("force_exited"):
                        self._emergency_exit(ticker, f"{ticker} split {split.SplitFactor}")

    # ── Order fill tracking ──────────────────────────────────────────────────

    def OnOrderEvent(self, orderEvent):
        # ── Detect QC auto-liquidation at SUBMITTED time (split / delisting) ──
        if orderEvent.Status == OrderStatus.Submitted:
            symbol = orderEvent.Symbol
            for ticker, ts in self._ts.items():
                if ts["state"] != "ACTIVE" or ts.get("force_exited"):
                    continue
                if symbol == ts.get("put_symbol") and self._filling_ticker != ticker:
                    order = self.Transactions.GetOrderById(orderEvent.OrderId)
                    if order is not None and order.Type == OrderType.MarketOnClose:
                        ts["force_exited"] = True
                        self.Log(f"  [{self.Time.date()}] SPLIT/DELISTING MOC submitted "
                                 f"for {ticker} put — closing stock immediately")
                        actual_qty = 0
                        if self.Portfolio.ContainsKey(ts["stock_symbol"]):
                            actual_qty = self.Portfolio[ts["stock_symbol"]].Quantity
                        if actual_qty != 0:
                            self._filling_ticker = ticker
                            self.MarketOrder(ts["stock_symbol"], -actual_qty)
                            self._filling_ticker = None
            return

        if orderEvent.Status != OrderStatus.Filled:
            return

        symbol     = orderEvent.Symbol
        fill_price = orderEvent.FillPrice
        fill_qty   = orderEvent.FillQuantity   # +buy / −sell

        for ticker, ts in self._ts.items():
            if ts["state"] not in ("ACTIVE", "EMERGENCY_PENDING"):
                continue

            # ── Put fill ──────────────────────────────────────────────
            if symbol == ts.get("put_symbol"):
                if fill_qty > 0:
                    ts["put_entry_fill"] = fill_price
                    # Place deferred stock hedge now that put is filled
                    pending = ts.get("pending_hedge_shares", 0)
                    if pending > 0:
                        self._filling_ticker = ticker
                        self.MarketOrder(ts["stock_symbol"], pending)
                        self._filling_ticker = None
                        ts["pending_hedge_shares"] = 0
                else:
                    ts["put_exit_fill"] = fill_price
                    if ts.get("force_exited"):
                        # Stock already closed at Submitted time — finalize P&L
                        n = ts["put_contracts"]
                        put_pnl = (fill_price - ts["put_entry_fill"]) * n * 100
                        stk_pnl = ts["stock_realized"]
                        total   = put_pnl + stk_pnl
                        entry_px = ts["stock_entry_price"]
                        exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
                        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0
                        ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()
                        iv_min_date = ts.get("iv_min_date")
                        iv_min_days = (ed - iv_min_date).days if iv_min_date else 0
                        iv_max_date = ts.get("iv_max_date")
                        iv_max_days = (ed - iv_max_date).days if iv_max_date else 0
                        ts["trade_log"].append({
                            "earnings":           ed,
                            "put_pnl":            put_pnl,
                            "stk_pnl":            stk_pnl,
                            "stk_chg_pct":        stk_chg_pct,
                            "total":              total,
                            "iv_early_put":       ts["iv_early_put"],
                            "iv_entry":           ts["put_entry_iv"],
                            "iv_exit":            0.0,
                            "rv":                 ts["entry_rv"],
                            "iv_min":             ts.get("iv_min", 0.0) or 0.0,
                            "iv_min_days_before": iv_min_days,
                            "iv_max":             ts.get("iv_max", 0.0) or 0.0,
                            "iv_max_days_before": iv_max_days,
                            "sim_pnl":            0.0,
                            "put_spread_entry":   ts["put_spread_entry"],
                            "put_spread_exit":    0,
                            "call_spread_exit":   0,
                            "hedge_count":        ts["hedge_count"],
                            "iv_rank":            ts["entry_iv_rank"],
                            "iv_rank_exit":       ts["exit_iv_rank"],
                            "iv_enter_sample":    ts["iv_enter_sample"],
                            "iv_exit_sample":     ts["iv_exit_sample"],
                            "entry_iv_pctl":      ts["entry_iv_pctl"],
                            "exit_iv_pctl":       ts["exit_iv_pctl"],
                            "vix_entry":          ts["vix_entry"],
                            "vix_exit":           ts["vix_exit"],
                        })
                        self.Log(f"  [{self.Time.date()}] FORCED EXIT FINALIZED: "
                                 f"{ticker} put=${fill_price:.2f} "
                                 f"putPnL=${put_pnl:+,.0f} stkPnL=${stk_pnl:+,.0f} "
                                 f"total=${total:+,.0f}")
                        ts["hedge_count"] = 0
                        self._reset(ticker)
                        return
                    # Detect auto-liquidation: put sold but NOT by our code
                    if self._filling_ticker != ticker:
                        self.Log(f"  [{self.Time.date()}] AUTO-LIQUIDATION detected: "
                                 f"put {symbol} sold at ${fill_price:.2f} (likely stock split)")
                        self._emergency_exit(ticker, "stock split / auto-liquidation")
                return

            # ── Stock fill ────────────────────────────────────────────
            if symbol == ts["stock_symbol"]:
                if fill_qty > 0:
                    ts["stock_cost_basis"] += fill_qty * fill_price
                    ts["stock_qty"]        += fill_qty
                else:
                    sold = abs(fill_qty)
                    if ts["stock_qty"] > 0:
                        avg_cost = ts["stock_cost_basis"] / ts["stock_qty"]
                    else:
                        avg_cost = fill_price
                    ts["stock_realized"]   += (fill_price - avg_cost) * sold
                    ts["stock_cost_basis"] -= avg_cost * sold
                    ts["stock_qty"]        -= sold
                return

    # ── Position management (10:00 AM — 30 min after open) ────────────────────

    def _manage_position(self, ticker):
        ts = self._ts[ticker]

        # ── Finalize any emergency exit from a stock split ──────────
        if ts["state"] == "EMERGENCY_PENDING":
            self._finalize_emergency_exit(ticker)
            return

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

        # Sanity check: skip if put price is unreasonably high vs underlying
        if MAX_PUT_PCT > 0 and put_mid > s_price * MAX_PUT_PCT:
            self.Log(f"  [{ticker}] SKIP — put_mid ${put_mid:.2f} > "
                     f"{MAX_PUT_PCT:.0%} of stock ${s_price:.2f}")
            return

        n_contracts = max(1, int(S / (put_mid * 100)))

        # ── Bid-ask spread cutoff (percentage of mid) ────────────────────────
        put_spread_raw = abs(put.BidPrice - put.AskPrice)
        spread_pct = put_spread_raw / put_mid if put_mid > 0 else 999
        put_spread_cost = put_spread_raw * 100 * n_contracts
        if SPREAD_CUTOFF_PCT > 0 and spread_pct > SPREAD_CUTOFF_PCT:
            self.Log(f"  [{ticker}] SKIP — spread too wide "
                     f"({spread_pct:.1%} of mid, cutoff={SPREAD_CUTOFF_PCT:.0%})")
            return
        ts["put_spread_entry"] = round(put_spread_cost)

        # ── Realized volatility + IV/RV filter ───────────────────────────────
        rv = self._calc_realized_vol(ts["stock_symbol"], 30)

        try:
            cur_iv = put.ImpliedVolatility
        except Exception:
            cur_iv = 0.0

        # ── IV term-structure: find same-strike put expiring ~1 week earlier ──
        early_iv = 0.0
        target_expiry = put.Expiry.date() - timedelta(days=7)
        candidates = [
            c for c in ts["chain"]
            if c.Right == OptionRight.Put
            and c.Strike == put.Strike
            and c.Expiry.date() < put.Expiry.date()
        ]
        if candidates:
            best = min(candidates, key=lambda c: abs((c.Expiry.date() - target_expiry).days))
            try:
                early_iv = best.ImpliedVolatility
            except Exception:
                early_iv = 0.0
        ts["iv_early_put"] = early_iv

        # Compute IV rank at entry using ~90 DTE put (same tenor as daily sampling)
        today = self.Time.date()
        rank_iv = 0.0
        iv_rank_put = self._put_for_iv_rank(ts["chain"], today, s_price)
        if iv_rank_put is not None:
            try:
                rank_iv = iv_rank_put.ImpliedVolatility
            except Exception:
                rank_iv = 0.0
        if rank_iv <= 0 and ts["iv_history"]:
            rank_iv = ts["iv_history"][-1][1]
        if rank_iv > 0:
            ts["entry_iv_rank"] = self._compute_iv_rank(ticker, rank_iv)
            ts["iv_enter_sample"] = rank_iv
            ts["entry_iv_pctl"] = self._compute_iv_percentile(ticker, rank_iv)

        if Z > 0 and rv > 0:
            if cur_iv / rv >= Z:
                return

        delta    = put.Greeks.Delta
        n_shares = max(0, round(abs(n_contracts * 100 * delta)))

        # Activate BEFORE placing orders so OnOrderEvent can track fills
        ts["state"]            = "ACTIVE"
        ts["put_symbol"]       = put.Symbol
        ts["put_contracts"]    = n_contracts
        ts["put_entry_fill"]   = 0.0
        ts["put_exit_fill"]    = 0.0
        ts["put_entry_iv"]     = cur_iv
        ts["stock_qty"]        = 0
        ts["stock_cost_basis"] = 0.0
        ts["stock_realized"]   = 0.0
        ts["last_hedge_price"]  = s_price
        ts["stock_entry_price"] = s_price
        ts["entry_earnings"]    = earnings_dt
        ts["entry_rv"]          = rv
        ts["iv_min"]            = ts["put_entry_iv"]
        ts["iv_min_date"]       = self.Time.date()
        ts["iv_max"]            = ts["put_entry_iv"]
        ts["iv_max_date"]       = self.Time.date()

        # Lock put subscription so contract stays in the chain even if price drifts from strike
        _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
        self.AddOptionContract(put.Symbol, _res)

        # Subscribe to the call at the same strike/expiry as the put (for sim PnL at exit)
        call_symbol = Symbol.CreateOption(
            ts["stock_symbol"],
            put.Symbol.ID.Market,
            OptionStyle.American,
            OptionRight.Call,
            put.Strike,
            put.Expiry,
        )
        self.AddOptionContract(call_symbol, _res)
        ts["call_symbol"] = call_symbol

        # Place limit order for put; stock hedge deferred until put fills
        ts["pending_hedge_shares"] = n_shares
        limit_px = round(put_mid * PUT_LIMIT_MULT, 2)
        ticket = self.LimitOrder(put.Symbol, n_contracts, limit_px)
        ts["put_order_ticket"] = ticket

        # Track peak concurrent positions
        active = sum(1 for t in self._ts.values() if t["state"] == "ACTIVE")
        if active > self._max_concurrent:
            self._max_concurrent = active

        vix_px = self.Securities[self._vix_symbol].Price
        ts["vix_entry"] = vix_px if vix_px > 0 else None

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _exit_position(self, ticker):
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE":
            return
        if ts.get("force_exited"):
            return   # already emergency-closed (e.g. stock split)

        vix_px = self.Securities[self._vix_symbol].Price
        ts["vix_exit"] = vix_px if vix_px > 0 else None

        # Read exit IV from chain (for logging + min/max tracking)
        put_exit_iv = 0.0
        if ts["chain"]:
            for c in ts["chain"]:
                if c.Symbol == ts["put_symbol"]:
                    try:
                        put_exit_iv = c.ImpliedVolatility
                    except Exception:
                        put_exit_iv = 0.0
                    break

        # Update IV min/max with exit-day reading
        if put_exit_iv > 0:
            if ts["iv_min"] is None or put_exit_iv < ts["iv_min"]:
                ts["iv_min"] = put_exit_iv
                ts["iv_min_date"] = self.Time.date()
            if ts["iv_max"] is None or put_exit_iv >= ts["iv_max"]:
                ts["iv_max"] = put_exit_iv
                ts["iv_max_date"] = self.Time.date()

        # Compute IV rank at exit using ~90 DTE put (same tenor as daily sampling)
        today = self.Time.date()
        s_price = self.Securities[ts["stock_symbol"]].Price
        exit_rank_iv = 0.0
        exit_rank_put = self._put_for_iv_rank(ts["chain"], today, s_price)
        if exit_rank_put is not None:
            try:
                exit_rank_iv = exit_rank_put.ImpliedVolatility
            except Exception:
                exit_rank_iv = 0.0
        if exit_rank_iv <= 0 and ts["iv_history"]:
            exit_rank_iv = ts["iv_history"][-1][1]
        if exit_rank_iv > 0:
            ts["exit_iv_rank"] = self._compute_iv_rank(ticker, exit_rank_iv)
            ts["iv_exit_sample"] = exit_rank_iv
            ts["exit_iv_pctl"] = self._compute_iv_percentile(ticker, exit_rank_iv)

        # Save quantities before orders (OnOrderEvent will zero them out)
        n_contracts = ts["put_contracts"]
        n_shares    = ts["stock_qty"]

        # Record exit bid-ask spread costs before closing
        try:
            _put_sec = self.Securities[ts["put_symbol"]]
            ts["put_spread_exit"] = round(abs(_put_sec.BidPrice - _put_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            ts["put_spread_exit"] = 0

        # Place exit orders — OnOrderEvent captures actual fill prices
        self._filling_ticker = ticker
        if n_contracts > 0:
            self.MarketOrder(ts["put_symbol"], -n_contracts)
        if n_shares > 0:
            self.MarketOrder(ts["stock_symbol"], -n_shares)
        self._filling_ticker = None

        # PnL from actual fill prices (set by OnOrderEvent)
        put_pnl   = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
        stk_pnl   = ts["stock_realized"]
        total_pnl = put_pnl + stk_pnl

        # ── Sim PnL (fair-value put exit + stock hedge PnL) ──────────────
        s_price = self.Securities[ts["stock_symbol"]].Price
        strike  = self.Securities[ts["put_symbol"]].Symbol.ID.StrikePrice
        call_sym = ts.get("call_symbol")

        if s_price >= strike:
            effective_exit = _mid(_put_sec.BidPrice, _put_sec.AskPrice)
        else:
            call_mid = 0.0
            if call_sym and self.Securities.ContainsKey(call_sym):
                _call_sec = self.Securities[call_sym]
                call_mid = _mid(_call_sec.BidPrice, _call_sec.AskPrice)
                ts["call_spread_exit"] = round(abs(_call_sec.BidPrice - _call_sec.AskPrice) * 100 * n_contracts)
            effective_exit = (strike - s_price) + call_mid

        if effective_exit > 0 and ts["put_entry_fill"] > 0:
            sim_pnl = (effective_exit - ts["put_entry_fill"]) * n_contracts * 100 + stk_pnl
        else:
            sim_pnl = 0.0

        # Stock % change
        entry_px = ts["stock_entry_price"]
        exit_px  = s_price
        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        ed = ts["entry_earnings"].date()

        iv_min_date = ts.get("iv_min_date")
        iv_min_days = (ed - iv_min_date).days if iv_min_date else 0
        iv_max_date = ts.get("iv_max_date")
        iv_max_days = (ed - iv_max_date).days if iv_max_date else 0

        ts["trade_log"].append({
            "earnings":           ed,
            "put_pnl":            put_pnl,
            "stk_pnl":            stk_pnl,
            "stk_chg_pct":        stk_chg_pct,
            "total":              total_pnl,
            "iv_early_put":       ts["iv_early_put"],
            "iv_entry":           ts["put_entry_iv"],
            "iv_exit":            put_exit_iv,
            "rv":                 ts["entry_rv"],
            "iv_min":             ts.get("iv_min", 0.0) or 0.0,
            "iv_min_days_before": iv_min_days,
            "iv_max":             ts.get("iv_max", 0.0) or 0.0,
            "iv_max_days_before": iv_max_days,
            "sim_pnl":            sim_pnl,
            "put_spread_entry":   ts["put_spread_entry"],
            "put_spread_exit":    ts["put_spread_exit"],
            "call_spread_exit":   ts["call_spread_exit"],
            "hedge_count":        ts["hedge_count"],
            "iv_rank":            ts["entry_iv_rank"],
            "iv_rank_exit":       ts["exit_iv_rank"],
            "iv_enter_sample":    ts["iv_enter_sample"],
            "iv_exit_sample":     ts["iv_exit_sample"],
            "entry_iv_pctl":      ts["entry_iv_pctl"],
            "exit_iv_pctl":       ts["exit_iv_pctl"],
            "vix_entry":          ts["vix_entry"],
            "vix_exit":           ts["vix_exit"],
        })
        ts["hedge_count"] = 0
        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    # ── Delta hedge V2: absolute-delta tolerance (30 min before close) ────────

    def _delta_hedge(self, ticker):
        ts = self._ts[ticker]

        self._sample_iv(ticker)

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

        # ── Cancel unfilled put limit order if still pending ─────────────
        if ts["state"] == "ACTIVE" and ts.get("put_entry_fill", 0) == 0:
            ticket = ts.get("put_order_ticket")
            if ticket is not None:
                ticket.Cancel()
                self.Log(f"  [{ticker}] Limit order for put did not fill — cancelling and resetting")
                self._reset(ticker)
                return

        # Keep put+call subscribed so they stay in the chain regardless of price drift
        if ts["state"] == "ACTIVE" and ts.get("put_symbol") is not None:
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            self.AddOptionContract(ts["put_symbol"], _res)
            if ts.get("call_symbol"):
                self.AddOptionContract(ts["call_symbol"], _res)

        if ts["state"] != "ACTIVE" or ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        # ── Step 1: Compute live position delta ──────────────────────────────
        # Find the current put Greeks from the live chain
        cur_delta = None
        cur_gamma = None
        cur_theta = None
        cur_iv    = 0.0
        for c in ts["chain"]:
            if c.Symbol == ts["put_symbol"]:
                cur_delta = c.Greeks.Delta
                cur_gamma = c.Greeks.Gamma
                cur_theta = c.Greeks.Theta
                try:
                    cur_iv = c.ImpliedVolatility
                except Exception:
                    cur_iv = 0.0
                break

        if cur_delta is None:
            return

        if cur_delta == 0.0 and cur_iv == 0.0:
            return

        # Track IV min / max over the life of the trade
        if cur_iv > 0:
            if ts["iv_min"] is None or cur_iv < ts["iv_min"]:
                ts["iv_min"] = cur_iv
                ts["iv_min_date"] = self.Time.date()
            if ts["iv_max"] is None or cur_iv >= ts["iv_max"]:
                ts["iv_max"] = cur_iv
                ts["iv_max_date"] = self.Time.date()

        # Stock delta = number of shares (each share has delta +1)
        stock_delta = ts["stock_qty"]

        # Option delta contribution = put_delta × contracts × 100
        # (put_delta is negative, so this is negative for long puts)
        option_delta = cur_delta * ts["put_contracts"] * 100

        # Net position delta (should be near zero if well-hedged)
        position_delta = stock_delta + option_delta

        # ── Step 2: Hedge trigger ──────────────────────────────────────────────
        option_exposure = abs(ts["put_contracts"] * 100)

        if HEDGE_MODE in ("gamma", "theta"):
            # Determine PnL tolerance: fixed (gamma) or theta-scaled (theta)
            if HEDGE_MODE == "theta":
                # QC Greeks.Theta is annualized; convert to per-day
                daily_theta_pos = abs(cur_theta * ts["put_contracts"] * 100) / 365.0 if cur_theta else 0.0
                pnl_tol = max(THETA_K * daily_theta_pos, MIN_TOLERANCE)
            else:
                pnl_tol = PNL_TOLERANCE

            # ΔS trigger = √(2 × pnl_tol / Γ_position)
            total_gamma = abs(cur_gamma * ts["put_contracts"] * 100) if cur_gamma else 0.0

            if total_gamma <= 1e-10:
                # Gamma ≈ 0 → option is effectively dead (deep OTM near expiry).
                # Fall through only if residual shares need unwinding.
                if abs(position_delta) <= 1:
                    return
                # else: unwind leftover hedge shares below
            else:
                delta_s_trigger = (2.0 * pnl_tol / total_gamma) ** 0.5
                stock_move = abs(s_price - ts["last_hedge_price"])

                # Time-decay fallback: re-hedge if delta drift exceeds floor
                # even when the stock hasn't moved (charm / passage of time)
                max_drift = DRIFT_FLOOR * option_exposure
                if abs(position_delta) > max_drift:
                    pass  # fall through to hedge
                elif stock_move < delta_s_trigger:
                    return  # within PnL tolerance — do nothing

        else:
            # Original sigma-based tolerance
            if RV_SIGMA:
                rv = self._calc_realized_vol(ts["stock_symbol"], 30)
                if rv <= 0:
                    return
                daily_sigma_frac = rv / (252 ** 0.5)
            else:
                sigma_iv = cur_iv if cur_iv > 0 else ts["put_entry_iv"]
                if sigma_iv <= 0:
                    return
                daily_sigma_frac = sigma_iv / (252 ** 0.5)

            tolerance = D_mult * daily_sigma_frac * option_exposure
            if abs(position_delta) <= tolerance:
                return  # within tolerance — do nothing

        # Re-hedge to delta-neutral: target stock qty offsets option delta
        target = max(0, round(abs(option_delta)))
        adj    = target - ts["stock_qty"]
        if adj == 0:
            return

        self._filling_ticker = ticker
        self.MarketOrder(ts["stock_symbol"], adj)
        self._filling_ticker = None
        # OnOrderEvent handles stock_qty, stock_cost_basis, stock_realized

        ts["hedge_count"] += 1
        ts["last_hedge_price"] = s_price

    # ── IV Sampling + IV Rank ─────────────────────────────────────────────────

    def _put_for_iv_rank(self, chain, today, stock_price):
        """Earliest expiry with DTE >= IV_RANK_TARGET_DTE; OTM/ATM puts (strike <= spot),
        highest strike first, walk down until ImpliedVolatility > 0.
        Tries up to 2 expiry dates before giving up."""
        if chain is None or stock_price <= 0:
            return None
        floor = today + timedelta(days=IV_RANK_TARGET_DTE)
        eligible = [c for c in chain
                    if c.Right == OptionRight.Put
                    and c.Expiry.date() >= floor]
        if not eligible:
            return None
        expiries = sorted(set(c.Expiry for c in eligible))[:2]
        for exp in expiries:
            otm = [c for c in eligible
                   if c.Expiry == exp and c.Strike <= stock_price]
            if not otm:
                continue
            for strike in sorted(set(c.Strike for c in otm), reverse=True):
                p = next(c for c in otm if c.Strike == strike)
                try:
                    iv = p.ImpliedVolatility
                except Exception:
                    iv = 0.0
                if iv > 0:
                    return p
        return None

    def _sample_iv(self, ticker):
        """Sample IV from _put_for_iv_rank and append to history (once per day).
        Falls back to the previous day's IV if the chain lookup fails."""
        ts = self._ts[ticker]
        today = self.Time.date()

        if ts["last_iv_sample_date"] == today:
            return

        iv = 0.0
        if ts["chain"] is not None:
            stock = self.Securities[ts["stock_symbol"]]
            s_price = stock.Price
            if s_price > 0:
                p_iv = self._put_for_iv_rank(ts["chain"], today, s_price)
                if p_iv is not None:
                    try:
                        iv = p_iv.ImpliedVolatility
                    except Exception:
                        iv = 0.0

        if iv <= 0:
            if self.IsWarmingUp:
                ts["iv_sample_fails_warmup"] += 1
            else:
                ts["iv_sample_fails"] += 1
            if ts["iv_history"]:
                iv = ts["iv_history"][-1][1]

        if iv > 0:
            ts["iv_history"].append((today, iv))
            ts["last_iv_sample_date"] = today

    def _iv_rank_bounds(self, ticker):
        """Return (smoothed_min, smoothed_max) from iv_history, or (None, None)."""
        ts = self._ts[ticker]
        if len(ts["iv_history"]) < IV_LOOKBACK:
            return None, None
        vals = sorted(iv for _, iv in ts["iv_history"])
        n = min(IV_RANK_SMOOTH_N, len(vals) // 4)
        n = max(n, 1)
        return sum(vals[:n]) / n, sum(vals[-n:]) / n

    def _compute_iv_rank(self, ticker, current_iv):
        """Compute IV Rank from the stored history. Returns 0-100 or None if insufficient data."""
        iv_min, iv_max = self._iv_rank_bounds(ticker)
        if iv_min is None or iv_max <= iv_min:
            return None if iv_min is None else 0.0
        return (current_iv - iv_min) / (iv_max - iv_min) * 100.0

    def _compute_iv_percentile(self, ticker, current_iv):
        """IV Percentile: % of days in iv_history where IV was below current_iv."""
        ts = self._ts[ticker]
        if len(ts["iv_history"]) < IV_LOOKBACK:
            return None
        below = sum(1 for _, iv in ts["iv_history"] if iv < current_iv)
        return below / len(ts["iv_history"]) * 100.0

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
            return None
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

    # ── Emergency exit (stock split / auto-liquidation) ─────────────────────

    def _emergency_exit(self, ticker, reason):
        """Mark one ticker's position as EMERGENCY_PENDING.
        Does NOT place orders or calculate P&L — deferred to _finalize_emergency_exit."""
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE" or ts.get("force_exited"):
            return
        ts["force_exited"] = True
        ts["state"]        = "EMERGENCY_PENDING"
        self.Log(f"  [{self.Time.date()}] EMERGENCY EXIT ({reason}): "
                 f"ticker={ticker} (pending close)")

    def _finalize_emergency_exit(self, ticker):
        """Called from _manage_position when state is EMERGENCY_PENDING.
        Sells remaining stock, calculates P&L, logs the trade, and resets."""
        ts = self._ts[ticker]
        if ts["state"] != "EMERGENCY_PENDING":
            return

        self.Log(f"  [{self.Time.date()}] FINALIZING EMERGENCY EXIT for {ticker}")

        # ── Close remaining stock position ──────────────────────────
        actual_stock_qty = 0
        if self.Portfolio.ContainsKey(ts["stock_symbol"]):
            actual_stock_qty = self.Portfolio[ts["stock_symbol"]].Quantity

        if actual_stock_qty != 0:
            self._filling_ticker = ticker
            self.MarketOrder(ts["stock_symbol"], -actual_stock_qty)
            self._filling_ticker = None

        # ── P&L calculation ─────────────────────────────────────────
        n_contracts = ts["put_contracts"]
        put_pnl = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
        stk_pnl = ts["stock_realized"]
        total   = put_pnl + stk_pnl

        self.Log(f"  [{self.Time.date()}]  PutPnL=${put_pnl:+,.2f} StkPnL=${stk_pnl:+,.2f} Total=${total:+,.2f}")

        # Stock % change
        entry_px = ts["stock_entry_price"]
        exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()

        iv_min_date = ts.get("iv_min_date")
        iv_min_days = (ed - iv_min_date).days if iv_min_date else 0
        iv_max_date = ts.get("iv_max_date")
        iv_max_days = (ed - iv_max_date).days if iv_max_date else 0

        ts["trade_log"].append({
            "earnings":           ed,
            "put_pnl":            put_pnl,
            "stk_pnl":            stk_pnl,
            "stk_chg_pct":        stk_chg_pct,
            "total":              total,
            "iv_early_put":       ts["iv_early_put"],
            "iv_entry":           ts["put_entry_iv"],
            "iv_exit":            0.0,
            "rv":                 ts["entry_rv"],
            "iv_min":             ts.get("iv_min", 0.0) or 0.0,
            "iv_min_days_before": iv_min_days,
            "iv_max":             ts.get("iv_max", 0.0) or 0.0,
            "iv_max_days_before": iv_max_days,
            "sim_pnl":            0.0,
            "put_spread_entry":   ts["put_spread_entry"],
            "put_spread_exit":    0,
            "call_spread_exit":   0,
            "hedge_count":        ts["hedge_count"],
            "iv_rank":            ts["entry_iv_rank"],
            "iv_rank_exit":       ts["exit_iv_rank"],
            "iv_enter_sample":    ts["iv_enter_sample"],
            "iv_exit_sample":     ts["iv_exit_sample"],
            "entry_iv_pctl":      ts["entry_iv_pctl"],
            "exit_iv_pctl":       ts["exit_iv_pctl"],
            "vix_entry":          ts["vix_entry"],
            "vix_exit":           ts["vix_exit"],
        })
        ts["hedge_count"] = 0
        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    def _reset(self, ticker):
        ts = self._ts[ticker]
        ts["state"]            = "FLAT"
        ts["put_symbol"]       = None
        ts["put_contracts"]    = 0
        ts["put_entry_fill"]   = 0.0
        ts["put_exit_fill"]    = 0.0
        ts["put_entry_iv"]     = 0.0
        ts["put_order_ticket"] = None
        ts["pending_hedge_shares"] = 0
        ts["stock_qty"]        = 0
        ts["stock_cost_basis"] = 0.0
        ts["stock_realized"]   = 0.0
        ts["last_hedge_price"]  = 0.0
        ts["stock_entry_price"] = 0.0
        ts["entry_earnings"]    = None
        ts["iv_samples"]       = []
        ts["iv_avg"]           = None
        ts["scan_earnings"]    = None
        ts["entry_rv"]         = 0.0
        ts["iv_min"]           = None
        ts["iv_min_date"]      = None
        ts["iv_max"]           = None
        ts["iv_max_date"]      = None
        ts["force_exited"]     = False
        ts["call_symbol"]      = None
        ts["iv_early_put"]     = 0.0
        ts["put_spread_entry"] = 0
        ts["put_spread_exit"]  = 0
        ts["call_spread_exit"] = 0
        ts["entry_iv_rank"]    = None
        ts["exit_iv_rank"]     = None
        ts["iv_enter_sample"]  = None
        ts["iv_exit_sample"]   = None
        ts["entry_iv_pctl"]    = None
        ts["exit_iv_pctl"]     = None
        ts["vix_entry"]        = None
        ts["vix_exit"]         = None

    # ── End-of-backtest summary ───────────────────────────────────────────────

    def _ol(self, lines, msg):
        """Log a message AND collect it for ObjectStore persistence."""
        self.Log(msg)
        lines.append(msg)

    def OnEndOfAlgorithm(self):
        lines = []          # collected for ObjectStore (bypasses 100 KB log cap)
        grand_total = 0.0

        grand_trades = 0
        grand_wins   = 0
        grand_hedges = 0

        for ticker in self._ts:
            trade_log = self._ts[ticker]["trade_log"]
            n         = len(trade_log)

            self._ol(lines, f"{'═'*72}")
            if n == 0:
                self._ol(lines, f"  {ticker} SUMMARY  |  No trades completed")
                self._ol(lines, f"{'═'*72}")
                continue

            totals = {"put": 0.0, "stk": 0.0, "total": 0.0, "sim": 0.0}
            valid  = [t for t in trade_log if t["iv_exit"] != 0.0]
            wins   = sum(1 for t in valid if t["total"] >= 0)
            nv     = len(valid)
            grand_trades += nv
            grand_wins   += wins

            self._ol(lines, f"  {ticker} SUMMARY  |  {nv} trade(s)  |  Wins: {wins}/{nv}  (skipped {n - nv} w/ iv_exit=0)")
            self._ol(lines, f"{'─'*72}")
            self._ol(lines,
                f"  {'Earnings':<12}"
                f" {'Put PnL':>12} {'Stock PnL':>12} {'Stk Chg%':>9} {'Combined':>12}"
                f" {'SimPnL':>12}"
                f" {'IVdif':>7}"
                f" {'pIVEn':>6} {'pIVEx':>6}"
                f" {'IVR':>5} {'IVRex':>6}"
                f" {'IVsEn':>7} {'IVsEx':>7}"
                f" {'IV entry':>9} {'IV exit':>8} {'VIXen':>6} {'VIXex':>6} {'IV min':>7} {'IV max':>7}"
                f" {'IV chg':>7} {'IV/RV':>6} {'neIVR':>6}"
                f" {'PSpEn':>7} {'PSpEx':>7} {'CSpEx':>7}"
            )
            self._ol(lines, f"  {'─'*166}")

            skipped = 0
            for t in trade_log:
                if t["iv_exit"] == 0.0:
                    skipped += 1
                    continue
                tag    = "[+]" if t["total"] >= 0 else "[-]"
                rv     = t.get("rv", 0.0)
                ratio  = f"{t['iv_entry'] / rv:.2f}" if rv > 0 else "n/a"
                iv_chg = (t['iv_exit'] - t['iv_entry']) / t['iv_entry'] * 100 if t['iv_entry'] > 0 else 0.0
                chg    = t.get("stk_chg_pct", 0.0)
                iv_min = t.get("iv_min", 0.0)
                iv_max = t.get("iv_max", 0.0)
                _sim   = t.get("sim_pnl", 0.0)
                _eiv     = t.get("iv_early_put", 0.0)
                _ivdif   = f"{(t['iv_entry'] - _eiv)*100:>5.1f}%" if _eiv > 0 else "    n/a"
                ne_ratio = f"{_eiv / rv:.2f}" if _eiv > 0 and rv > 0 else "n/a"
                _pse   = t.get("put_spread_entry", 0)
                _psx   = t.get("put_spread_exit", 0)
                _csx   = t.get("call_spread_exit", 0)
                _pivEn = t.get("entry_iv_pctl")
                _pivEx = t.get("exit_iv_pctl")
                _pivEn_s = f"{_pivEn:.0f}" if _pivEn is not None else "n/a"
                _pivEx_s = f"{_pivEx:.0f}" if _pivEx is not None else "n/a"
                _ivr   = t.get("iv_rank")
                _ivrex = t.get("iv_rank_exit")
                _ivr_s   = f"{_ivr:.0f}" if _ivr is not None else "n/a"
                _ivrex_s = f"{_ivrex:.0f}" if _ivrex is not None else "n/a"
                _ives  = t.get("iv_enter_sample")
                _ivxs  = t.get("iv_exit_sample")
                _ives_s = f"{_ives:.1%}" if _ives is not None else "  n/a"
                _ivxs_s = f"{_ivxs:.1%}" if _ivxs is not None else "  n/a"
                _vixen = t.get("vix_entry")
                _vixxs = t.get("vix_exit")
                _vixen_s = f"{_vixen:.1f}" if _vixen else "n/a"
                _vixxs_s = f"{_vixxs:.1f}" if _vixxs else "n/a"
                self._ol(lines,
                    f"  {tag} {t['earnings']!s:<11}"
                    f"  ${t['put_pnl']:>+10,.2f}"
                    f"  ${t['stk_pnl']:>+10,.2f}"
                    f"  {chg:>+7.1f}%"
                    f"  ${t['total']:>+10,.2f}"
                    f"  ${_sim:>+10,.2f}"
                    f"  {_ivdif:>7}"
                    f"  {_pivEn_s:>6} {_pivEx_s:>6}"
                    f"  {_ivr_s:>5} {_ivrex_s:>6}"
                    f"  {_ives_s:>7} {_ivxs_s:>7}"
                    f"  {t['iv_entry']:>8.1%}"
                    f"  {t['iv_exit']:>7.1%}"
                    f"  {_vixen_s:>6} {_vixxs_s:>6}"
                    f"  {iv_min:>6.1%}"
                    f"  {iv_max:>6.1%}"
                    f"  {iv_chg:>+6.0f}%"
                    f"  {ratio:>6}"
                    f"  {ne_ratio:>6}"
                    f"  {_pse:>7}"
                    f"  {_psx:>7}"
                    f"  {_csx:>7}"
                )
                totals["put"]   += t["put_pnl"]
                totals["stk"]   += t["stk_pnl"]
                totals["total"] += t["total"]
                totals["sim"]   += _sim

            printed = n - skipped
            avg = totals["total"] / printed if printed > 0 else 0.0
            self._ol(lines, f"  {'─'*150}")
            self._ol(lines,
                f"  {'TOTAL':<15}  ${totals['put']:>+10,.2f}"
                f"  ${totals['stk']:>+10,.2f}"
                f"  {'':>9}"
                f"  ${totals['total']:>+10,.2f}"
                f"  ${totals['sim']:>+10,.2f}"
            )
            ticker_hedges = sum(t.get("hedge_count", 0) for t in valid)
            avg_hedges = ticker_hedges / printed if printed > 0 else 0.0
            self._ol(lines, f"  Avg PnL/trade: ${avg:+,.2f}  |  Hedges: {ticker_hedges} total, {avg_hedges:.1f} avg/trade")
            self._ol(lines, f"{'═'*72}")
            grand_total  += totals["total"]
            grand_hedges += ticker_hedges

        if len(self._ts) > 1:
            self._ol(lines, f"{'═'*72}")
            self._ol(lines, f"  ALL TICKERS COMBINED  |  {grand_trades} trade(s)  |  Wins: {grand_wins}/{grand_trades}  |  Max concurrent positions: {self._max_concurrent}")
            avg_h = grand_hedges / grand_trades if grand_trades else 0.0
            grand_iv_fails = sum(ts["iv_sample_fails"] for ts in self._ts.values())
            grand_iv_fails_wu = sum(ts["iv_sample_fails_warmup"] for ts in self._ts.values())
            self._ol(lines, f"  Combined PnL: ${grand_total:+,.2f}  |  Avg PnL/trade: ${grand_total / grand_trades:+,.2f}  |  Hedges: {grand_hedges} total, {avg_h:.1f} avg/trade" if grand_trades else f"  Combined PnL: ${grand_total:+,.2f}")
            self._ol(lines, f"  IV sample fails: {grand_iv_fails} live, {grand_iv_fails_wu} warmup")
            self._ol(lines, f"{'═'*72}")

        # ── Persist full log to ObjectStore (no 100 KB cap) ──────────────
        self.ObjectStore.Save("backtest_logs", "\n".join(lines))


# ─── Mid-price fill model ─────────────────────────────────────────────────────

class NullAssignmentModel(DefaultOptionAssignmentModel):
    """Never trigger automatic option assignment — the algo manages all exits."""
    def GetAssignment(self, parameters):
        return OptionAssignmentResult.Null


class MidPriceFillModel(ImmediateFillModel):
    def MarketFill(self, asset, order):
        fill = ImmediateFillModel.MarketFill(self, asset, order)
        mid  = _mid(asset.BidPrice, asset.AskPrice)
        if mid > 0:
            fill.FillPrice = round(mid, 2)
        return fill

    def LimitFill(self, asset, order):
        mid = _mid(asset.BidPrice, asset.AskPrice)
        if mid <= 0:
            return super().LimitFill(asset, order)
        _no_fill = getattr(OrderStatus, 'None')   # avoid Python keyword
        # Buy limit: fill at mid only if mid ≤ limit price
        if order.Quantity > 0 and mid > order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid above limit")
        # Sell limit: fill at mid only if mid ≥ limit price
        if order.Quantity < 0 and mid < order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid below limit")
        # Condition met — fill at mid
        fill = super().LimitFill(asset, order)
        if fill.Status == OrderStatus.Filled:
            fill.FillPrice = round(mid, 2)
        return fill
