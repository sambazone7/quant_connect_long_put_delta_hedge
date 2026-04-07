# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
from collections import deque
import math
# endregion

# ─── Configurable Parameters ──────────────────────────────────────────────────

K               = 80     # Target days-to-expiry for the sold put (calendar days)
RANK_THRESHOLD  = 1      # Minimum IV Rank (0-100) to trigger entry
NDAYS_CLOSE     = 28     # Close after this many calendar days; also entry earnings window [today, today+NDAYS_CLOSE]
EARNINGS_BUFFER = 10     # (Reserved) documented for future use — not applied in entry logic today
SKIP_EARNINGS_GUARD = False  # True → skip the entry earnings check below
IV_LOOKBACK     = 160    # Trading days of IV history for IV Rank min/max
IV_RANK_SMOOTH_N = 10    # Average the N lowest/highest IV samples for rank min/max (1 = raw min/max)
IV_RANK_TARGET_DTE = 90  # IV rank: closest listed expiry with DTE >= this; OTM put (strike < spot), walk down
DAYS_AFTER_EARNINGS = 5  # Skip options whose expiry is within this many days after an earnings date
MIN_DTE_EXIT = 5         # Close position if put has <= this many trading days until expiry
IV_ROLLBACK_RATIO = 0    # If selected put IV / prior-week put IV > this, sell the prior-week put instead (0 = disabled)

S      = 10_000          # Notional USD value of puts to sell at entry (per ticker)
HEDGE_MODE = "theta"     # "gamma" → fixed PnL-tolerance trigger (ΔS = √(2·tol/Γ))
                         # "theta" → theta-scaled PnL trigger (tol = THETA_K × |daily θ|)
                         # "sigma" → original vol-scaled delta tolerance
PNL_TOLERANCE = 100      # (gamma mode) Dollar P&L threshold per position before re-hedging
THETA_K       = 1.0      # (theta mode) Scalar on daily theta: tol = THETA_K × |θ_daily_position|
                         # 1.0 = re-hedge at the theta-gamma breakeven move
                         # <1  = tighter (hedge before breakeven)  >1 = looser
MIN_TOLERANCE = 50       # (theta mode) Floor on dynamic tolerance to guard against near-zero theta
DRIFT_FLOOR   = 0.10     # (gamma/theta mode) Max |position_delta| as fraction of option exposure
                         # Catches time-decay drift when stock is flat (0.10 = 10%)
D_mult  = 1.0            # (sigma mode) tolerance = D_mult × daily_sigma_frac × |option_exposure|
RV_SIGMA = False          # (sigma mode) True → hedge tolerance from 30d RV; False → from put's live IV
MAX_PUT_PCT = 0.15       # Skip entry if put_mid > stock_price × MAX_PUT_PCT
SPREAD_CUTOFF_PCT = 0.20 # Max bid-ask spread as fraction of option mid price (0 = disabled)
PRICE_MODEL = "default"  # "BT" | "BS" | "default"
HOURLY_BARS = False       # True → Resolution.Hour; False → Resolution.Minute
TRADE_TIME_MIN = 270      # Minutes after market open to enter/exit trades
HEDGE_TIME_MIN = 15       # Minutes before market close to run delta hedge + IV sampling
DEBUG = 0                 # 1 → log every delta-hedge decision with Greeks; 0 → silent

# ─── Earnings Dates (universe definition) ────────────────────────────────────
from listqqqAll import MANUAL_EARNINGS_DATES

# ──────────────────────────────────────────────────────────────────────────────

def _mid(bid, ask):
    if bid > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return 0.0


class ShortPutIVRankAlgo(QCAlgorithm):
    """
    Multi-ticker Short-Put + Delta-Neutral Strategy (IV Rank entry)
    ================================================================
    Sells ATM puts when IV Rank is elevated, delta-hedges with short
    stock, and closes after NDAYS_CLOSE calendar days.

    IV Rank uses an OTM put (strike < spot) at the earliest listed expiry
    with calendar DTE >= IV_RANK_TARGET_DTE.

    Entry criteria:
      1. IV Rank (252-day lookback) >= RANK_THRESHOLD
      2. Unless SKIP_EARNINGS_GUARD: skip entry if any earnings in [today, today+NDAYS_CLOSE] (calendar)
      3. Not already holding a short put on this ticker
      4. Passes spread and price sanity filters

    Delta hedge (V2 absolute-delta tolerance):
      Same logic as the long-put algo but inverted — short stock
      offsets the positive delta of a short put.
    """

    # ── Initialise ────────────────────────────────────────────────────────────

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(20_000_000)

        self.SetWarmUp(timedelta(days=IV_LOOKBACK + 60))

        self._vix_symbol = self.AddData(CBOE, "VIX", Resolution.Daily).Symbol

        tickers = list(MANUAL_EARNINGS_DATES.keys())

        self._ts = {}
        self._max_concurrent = 0
        self._filling_ticker = None

        _exp_max = max(K, IV_RANK_TARGET_DTE) + 30

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

            earnings = self._load_earnings_dates(ticker)

            self._ts[ticker] = {
                "stock_symbol":     eq.Symbol,
                "option_symbol":    opt.Symbol,
                "state":            "FLAT",
                "put_symbol":       None,
                "put_contracts":    0,
                "put_entry_fill":   0.0,
                "put_exit_fill":    0.0,
                "put_entry_iv":     0.0,
                "stock_qty":        0,
                "stock_cost_basis": 0.0,
                "stock_realized":   0.0,
                "last_hedge_price": 0.0,
                "stock_entry_price": 0.0,
                "entry_date":       None,
                "entry_iv_rank":    0.0,
                "entry_rv":         0.0,
                "iv_hist_min":      0.0,
                "iv_hist_max":      0.0,
                "iv_trade_min":     None,
                "iv_trade_max":     None,
                "chain":            None,
                "iv_history":       deque(maxlen=IV_LOOKBACK),
                "last_iv_sample_date": None,
                "iv_sample_fails":  0,
                "earnings_dates":   earnings,
                "trade_log":        [],
                "force_exited":     False,
                "hedge_count":      0,
                "put_spread_entry": 0,
                "put_spread_exit":  0,
                "vix_entry":        None,
                "vix_exit":         None,
                "entry_iv_pctl":    None,
                "exit_iv_pctl":     None,
            }

            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.AfterMarketOpen(ticker, TRADE_TIME_MIN),
                lambda t=ticker: self._manage_position(t),
            )
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.BeforeMarketClose(ticker, HEDGE_TIME_MIN),
                lambda t=ticker: self._daily_hedge_and_sample(t),
            )

    # ── Earnings date loading ─────────────────────────────────────────────────

    def _load_earnings_dates(self, ticker):
        try:
            start = Date(self.StartDate.year,  self.StartDate.month,  self.StartDate.day)
        except AttributeError:
            start = Date(self.StartDate.Year,  self.StartDate.Month,  self.StartDate.Day)
        try:
            end = Date(self.EndDate.year, self.EndDate.month, self.EndDate.day)
        except AttributeError:
            end = Date(self.EndDate.Year, self.EndDate.Month, self.EndDate.Day)

        manual = MANUAL_EARNINGS_DATES.get(ticker, [])
        dates = []
        for d in manual:
            dd = d.date() if isinstance(d, datetime) else d
            if start <= dd <= end:
                dates.append(dd)
        return sorted(set(dates))

    # ── Fill model ────────────────────────────────────────────────────────────

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFillModel(MidPriceFillModel())

    # ── Data handler ──────────────────────────────────────────────────────────

    def OnData(self, data):
        for ticker, ts in self._ts.items():
            if ts["option_symbol"] in data.OptionChains:
                ts["chain"] = data.OptionChains[ts["option_symbol"]]

            if data.Splits.ContainsKey(ts["stock_symbol"]):
                split = data.Splits[ts["stock_symbol"]]
                if split.Type == SplitType.SplitOccurred:
                    if ts["state"] in ("ACTIVE", "EMERGENCY_PENDING"):
                        if ts["stock_qty"] != 0 and split.SplitFactor > 0:
                            ts["stock_qty"] = round(ts["stock_qty"] / split.SplitFactor)
                    if ts["state"] == "ACTIVE" and not ts.get("force_exited"):
                        self._emergency_exit(ticker, f"{ticker} split {split.SplitFactor}")

    # ── Order fill tracking ───────────────────────────────────────────────────

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
        fill_qty   = orderEvent.FillQuantity

        for ticker, ts in self._ts.items():
            if ts["state"] not in ("ACTIVE", "EMERGENCY_PENDING"):
                continue

            # ── Put fill ──────────────────────────────────────────────
            if symbol == ts.get("put_symbol"):
                if fill_qty < 0:
                    # Sold to open
                    ts["put_entry_fill"] = fill_price
                else:
                    # Bought to close
                    ts["put_exit_fill"] = fill_price
                    if ts.get("force_exited"):
                        # Stock already closed at Submitted time — finalize P&L
                        n = ts["put_contracts"]
                        put_pnl = (ts["put_entry_fill"] - fill_price) * n * 100
                        stk_pnl = ts["stock_realized"]
                        total   = put_pnl + stk_pnl
                        held_days = (self.Time.date() - ts["entry_date"]).days if ts["entry_date"] else 0
                        ts["trade_log"].append({
                            "entry_date":       ts["entry_date"],
                            "exit_date":        self.Time.date(),
                            "held_days":        held_days,
                            "put_pnl":          put_pnl,
                            "put_price_entry":  ts["put_entry_fill"],
                            "put_price_exit":   fill_price,
                            "stk_pnl":          stk_pnl,
                            "stk_chg_pct":      0.0,
                            "total":            total,
                            "iv_entry":         ts["put_entry_iv"],
                            "iv_exit":          0.0,
                            "iv_trade_min":     ts["iv_trade_min"] or 0.0,
                            "iv_trade_max":     ts["iv_trade_max"] or 0.0,
                            "iv_rank_entry":    ts["entry_iv_rank"],
                            "rv":               ts["entry_rv"],
                            "iv_hist_min":      ts["iv_hist_min"],
                            "iv_hist_max":      ts["iv_hist_max"],
                            "put_spread_entry": ts["put_spread_entry"],
                            "put_spread_exit":  0,
                            "vix_entry":        ts["vix_entry"],
                            "vix_exit":         ts["vix_exit"],
                            "entry_iv_pctl":    ts["entry_iv_pctl"],
                            "exit_iv_pctl":     ts["exit_iv_pctl"],
                            "hedge_count":      ts["hedge_count"],
                        })
                        self.Log(f"  [{self.Time.date()}] FORCED EXIT FINALIZED: "
                                 f"{ticker} put=${fill_price:.2f} "
                                 f"putPnL=${put_pnl:+,.0f} stkPnL=${stk_pnl:+,.0f} "
                                 f"total=${total:+,.0f}")
                        self._reset(ticker)
                        return
                    if self._filling_ticker != ticker:
                        self.Log(f"  [{self.Time.date()}] AUTO-LIQUIDATION detected: "
                                 f"put {symbol} bought at ${fill_price:.2f}")
                        self._emergency_exit(ticker, "auto-liquidation")
                return

            # ── Stock fill ────────────────────────────────────────────
            if symbol == ts["stock_symbol"]:
                if fill_qty > 0:
                    # Buying stock — may cover short, flip to long, or add to long
                    remaining = fill_qty
                    if ts["stock_qty"] < 0:
                        avg_short = abs(ts["stock_cost_basis"] / ts["stock_qty"])
                        cover = min(remaining, abs(ts["stock_qty"]))
                        ts["stock_realized"]   += (avg_short - fill_price) * cover
                        ts["stock_cost_basis"] += avg_short * cover
                        ts["stock_qty"]        += cover
                        remaining -= cover
                    if remaining > 0:
                        if ts["stock_qty"] > 0:
                            ts["stock_cost_basis"] += remaining * fill_price
                        else:
                            ts["stock_cost_basis"] = remaining * fill_price
                        ts["stock_qty"] += remaining
                elif fill_qty < 0:
                    # Selling stock — may close long, flip to short, or add to short
                    remaining = abs(fill_qty)
                    if ts["stock_qty"] > 0:
                        avg_long = ts["stock_cost_basis"] / ts["stock_qty"]
                        close = min(remaining, ts["stock_qty"])
                        ts["stock_realized"]   += (fill_price - avg_long) * close
                        ts["stock_cost_basis"] -= avg_long * close
                        ts["stock_qty"]        -= close
                        remaining -= close
                    if remaining > 0:
                        if ts["stock_qty"] < 0:
                            ts["stock_cost_basis"] -= remaining * fill_price
                        else:
                            ts["stock_cost_basis"] = -(remaining * fill_price)
                        ts["stock_qty"] -= remaining
                return

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
        """Sample IV from _put_for_iv_rank and append to history.
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

    # ── Position management (TRADE_TIME_MIN after open) ───────────────────────

    def _manage_position(self, ticker):
        if self.IsWarmingUp:
            return
        ts = self._ts[ticker]

        if ts["state"] == "EMERGENCY_PENDING":
            self._finalize_emergency_exit(ticker)
            return

        today = self.Time.date()

        # ── Exit check: close after NDAYS_CLOSE or near expiry ────────
        if ts["state"] == "ACTIVE":
            entry = ts["entry_date"]
            time_exit = entry and (today - entry).days >= NDAYS_CLOSE

            dte_exit = False
            if ts["put_symbol"] is not None:
                put_expiry = ts["put_symbol"].ID.Date.date()
                dte_trading = self._count_trading_days(ticker, today, put_expiry)
                dte_exit = dte_trading <= MIN_DTE_EXIT

            if time_exit or dte_exit:
                self._exit_position(ticker)
            return

        # ── Entry check (FLAT only) ──────────────────────────────────
        if ts["state"] != "FLAT":
            return
        if self.IsWarmingUp:
            return

        if ts["chain"] is None:
            return

        stock = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        # Need current IV for rank — same contract as _sample_iv
        current_iv = 0.0
        iv_put = self._put_for_iv_rank(ts["chain"], today, s_price)
        if iv_put is not None:
            try:
                current_iv = iv_put.ImpliedVolatility
            except Exception:
                current_iv = 0.0
        if current_iv <= 0 and ts["iv_history"]:
            current_iv = ts["iv_history"][-1][1]
        if current_iv <= 0:
            return

        iv_rank = self._compute_iv_rank(ticker, current_iv)
        if iv_rank is None:
            return
        if iv_rank < RANK_THRESHOLD:
            return

        # ── Select put to sell (expiry ~K days out) ──────────────────
        put = self._select_put(ts["chain"], today, s_price, ticker)
        if put is None:
            return

        # ── Earnings guard (entry only): do not open if earnings during planned NDAYS_CLOSE calendar hold ──
        if not SKIP_EARNINGS_GUARD:
            hold_end = today + timedelta(days=NDAYS_CLOSE)
            for ed in ts["earnings_dates"]:
                if today <= ed <= hold_end:
                    self.Log(
                        f"  [{ticker}] SKIP — earnings {ed} within hold window "
                        f"(entry {today} .. {hold_end}, NDAYS_CLOSE={NDAYS_CLOSE})"
                    )
                    return

        put_mid = _mid(put.BidPrice, put.AskPrice)
        if put_mid <= 0:
            return

        if MAX_PUT_PCT > 0 and put_mid > s_price * MAX_PUT_PCT:
            self.Log(f"  [{ticker}] SKIP — put_mid ${put_mid:.2f} > "
                     f"{MAX_PUT_PCT:.0%} of stock ${s_price:.2f}")
            return

        n_contracts = max(1, int(S / (put_mid * 100)))

        # ── Bid-ask spread filter ─────────────────────────────────────
        put_spread_raw = abs(put.BidPrice - put.AskPrice)
        spread_pct = put_spread_raw / put_mid if put_mid > 0 else 999
        put_spread_cost = put_spread_raw * 100 * n_contracts
        if SPREAD_CUTOFF_PCT > 0 and spread_pct > SPREAD_CUTOFF_PCT:
            self.Log(f"  [{ticker}] SKIP — spread too wide "
                     f"({spread_pct:.1%} of mid, cutoff={SPREAD_CUTOFF_PCT:.0%})")
            return
        ts["put_spread_entry"] = round(put_spread_cost)

        # ── Realized vol ──────────────────────────────────────────────
        rv = self._calc_realized_vol(ts["stock_symbol"], 30)

        try:
            entry_iv = put.ImpliedVolatility
        except Exception:
            entry_iv = current_iv

        delta = put.Greeks.Delta
        # Short put → net delta is positive (we sold negative-delta puts)
        # Hedge with short stock: sell shares = -|delta * contracts * 100|
        n_shares = -max(0, round(abs(n_contracts * 100 * delta)))

        # Activate state before placing orders
        ts["state"]             = "ACTIVE"
        ts["put_symbol"]        = put.Symbol
        ts["put_contracts"]     = n_contracts
        ts["put_entry_fill"]    = 0.0
        ts["put_exit_fill"]     = 0.0
        ts["put_entry_iv"]      = entry_iv
        ts["stock_qty"]         = 0
        ts["stock_cost_basis"]  = 0.0
        ts["stock_realized"]    = 0.0
        ts["last_hedge_price"]  = s_price
        ts["stock_entry_price"] = s_price
        ts["entry_date"]        = today
        ts["entry_iv_rank"]     = iv_rank
        ts["entry_iv_pctl"]     = self._compute_iv_percentile(ticker, current_iv)
        ts["entry_rv"]          = rv
        ts["iv_trade_min"]      = entry_iv
        ts["iv_trade_max"]      = entry_iv
        iv_lo, iv_hi = self._iv_rank_bounds(ticker)
        if iv_lo is not None:
            ts["iv_hist_min"] = iv_lo
            ts["iv_hist_max"] = iv_hi

        # Lock subscription so the contract stays in the chain even if price drifts from strike
        _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
        self.AddOptionContract(put.Symbol, _res)

        # Sell put + hedge immediately (MarketOrder fills at mid via MidPriceFillModel)
        self._filling_ticker = ticker
        self.MarketOrder(put.Symbol, -n_contracts)
        self.MarketOrder(ts["stock_symbol"], n_shares)
        self._filling_ticker = None

        ts["vix_entry"] = self._get_vix()

        active = sum(1 for t in self._ts.values() if t["state"] == "ACTIVE")
        if active > self._max_concurrent:
            self._max_concurrent = active

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _exit_position(self, ticker):
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE":
            return
        if ts.get("force_exited"):
            return

        ts["vix_exit"] = self._get_vix()

        if ts.get("put_symbol") is not None:
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            self.AddOptionContract(ts["put_symbol"], _res)

        put_exit_iv = 0.0
        if ts["chain"]:
            for c in ts["chain"]:
                if c.Symbol == ts["put_symbol"]:
                    try:
                        put_exit_iv = c.ImpliedVolatility
                    except Exception:
                        put_exit_iv = 0.0
                    break

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
            ts["exit_iv_pctl"] = self._compute_iv_percentile(ticker, exit_rank_iv)

        n_contracts = ts["put_contracts"]
        n_shares    = ts["stock_qty"]

        try:
            _put_sec = self.Securities[ts["put_symbol"]]
            ts["put_spread_exit"] = round(abs(_put_sec.BidPrice - _put_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            ts["put_spread_exit"] = 0

        # Buy back put, cover short stock
        self._filling_ticker = ticker
        if n_contracts > 0:
            self.MarketOrder(ts["put_symbol"], n_contracts)
        if n_shares != 0:
            self.MarketOrder(ts["stock_symbol"], -n_shares)
        self._filling_ticker = None

        # PnL: short put profits when exit_fill < entry_fill
        put_pnl   = (ts["put_entry_fill"] - ts["put_exit_fill"]) * n_contracts * 100
        stk_pnl   = ts["stock_realized"]
        total_pnl = put_pnl + stk_pnl

        s_price = self.Securities[ts["stock_symbol"]].Price
        entry_px = ts["stock_entry_price"]
        stk_chg_pct = ((s_price - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        held_days = (self.Time.date() - ts["entry_date"]).days if ts["entry_date"] else 0

        ts["trade_log"].append({
            "entry_date":       ts["entry_date"],
            "exit_date":        self.Time.date(),
            "held_days":        held_days,
            "put_pnl":          put_pnl,
            "put_price_entry":  ts["put_entry_fill"],
            "put_price_exit":   ts["put_exit_fill"],
            "stk_pnl":          stk_pnl,
            "stk_chg_pct":      stk_chg_pct,
            "total":            total_pnl,
            "iv_entry":         ts["put_entry_iv"],
            "iv_exit":          put_exit_iv,
            "iv_trade_min":     ts["iv_trade_min"] or 0.0,
            "iv_trade_max":     ts["iv_trade_max"] or 0.0,
            "iv_rank_entry":    ts["entry_iv_rank"],
            "rv":               ts["entry_rv"],
            "iv_hist_min":      ts["iv_hist_min"],
            "iv_hist_max":      ts["iv_hist_max"],
            "put_spread_entry": ts["put_spread_entry"],
            "put_spread_exit":  ts["put_spread_exit"],
            "vix_entry":        ts["vix_entry"],
            "vix_exit":         ts["vix_exit"],
            "entry_iv_pctl":    ts["entry_iv_pctl"],
            "exit_iv_pctl":     ts["exit_iv_pctl"],
            "hedge_count":      ts["hedge_count"],
        })
        self._reset(ticker)

    # ── Delta hedge + IV sampling (HEDGE_TIME_MIN before close) ───────────────

    def _daily_hedge_and_sample(self, ticker):
        ts = self._ts[ticker]

        # Always sample IV (even when FLAT / warming up)
        self._sample_iv(ticker)

        if self.IsWarmingUp:
            return

        # Keep put contract subscribed so it stays in the chain regardless of price drift
        if ts["state"] == "ACTIVE" and ts.get("put_symbol") is not None:
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            self.AddOptionContract(ts["put_symbol"], _res)

        if ts["state"] != "ACTIVE" or ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        # ── Live position delta ───────────────────────────────────────
        cur_delta = None
        cur_iv    = 0.0
        cur_gamma = 0.0
        cur_theta = 0.0
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

        if cur_delta is None or (cur_delta == 0.0 and cur_iv == 0.0):
            strike = ts["put_symbol"].ID.StrikePrice
            dist_pct = (s_price - strike) / strike * 100 if strike > 0 else 0
            self.Log(f"  [{self.Time.date()}] [{ticker}] WARN delta unavailable — "
                     f"stock ${s_price:.2f} vs strike ${strike:.2f} ({dist_pct:+.1f}%)")
            return

        # Track IV min/max over the life of the trade
        if cur_iv > 0:
            if ts["iv_trade_min"] is None or cur_iv < ts["iv_trade_min"]:
                ts["iv_trade_min"] = cur_iv
            if ts["iv_trade_max"] is None or cur_iv > ts["iv_trade_max"]:
                ts["iv_trade_max"] = cur_iv

        # Stock delta = stock_qty (negative if short)
        stock_delta = ts["stock_qty"]

        # Short put option delta: we sold puts, so our delta = -put_delta × contracts × 100
        # put_delta is negative → -negative = positive net delta
        option_delta = -cur_delta * ts["put_contracts"] * 100

        position_delta = stock_delta + option_delta

        # ── Hedge trigger ──────────────────────────────────────────────
        option_exposure = abs(ts["put_contracts"] * 100)

        if HEDGE_MODE in ("gamma", "theta"):
            if HEDGE_MODE == "theta":
                daily_theta_pos = abs(cur_theta * ts["put_contracts"] * 100) / 365.0 if cur_theta else 0.0
                pnl_tol = max(THETA_K * daily_theta_pos, MIN_TOLERANCE)
            else:
                pnl_tol = PNL_TOLERANCE

            total_gamma = abs(cur_gamma * ts["put_contracts"] * 100) if cur_gamma else 0.0

            if total_gamma <= 1e-10:
                if abs(position_delta) <= 1:
                    return
            else:
                delta_s_trigger = (2.0 * pnl_tol / total_gamma) ** 0.5
                stock_move = abs(s_price - ts["last_hedge_price"])

                max_drift = DRIFT_FLOOR * option_exposure
                if abs(position_delta) > max_drift:
                    pass  # fall through to hedge
                elif stock_move < delta_s_trigger:
                    if DEBUG:
                        strike = ts["put_symbol"].ID.StrikePrice
                        self.Log(f"  [{self.Time.date()}] [{ticker}] HEDGE SKIP "
                                 f"stk=${s_price:.2f} K=${strike:.0f} "
                                 f"delta={cur_delta:.4f} gamma={cur_gamma:.4f} "
                                 f"theta={cur_theta:.2f} IV={cur_iv:.1%} "
                                 f"posDelta={position_delta:.1f} "
                                 f"ΔS={stock_move:.2f} trigger={delta_s_trigger:.2f} "
                                 f"pnl_tol={pnl_tol:.1f}")
                    return

        else:
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
                if DEBUG:
                    strike = ts["put_symbol"].ID.StrikePrice
                    self.Log(f"  [{self.Time.date()}] [{ticker}] HEDGE SKIP "
                             f"stk=${s_price:.2f} K=${strike:.0f} "
                             f"delta={cur_delta:.4f} gamma={cur_gamma:.4f} "
                             f"theta={cur_theta:.2f} IV={cur_iv:.1%} "
                             f"posDelta={position_delta:.1f} tol={tolerance:.1f}")
                return

        # Target: stock qty should offset option delta → stock_target = -option_delta
        target = int(-round(option_delta))
        adj    = int(target - ts["stock_qty"])
        if adj == 0:
            return

        if DEBUG:
            strike = ts["put_symbol"].ID.StrikePrice
            _tol_s = f"pnl_tol={pnl_tol:.1f}" if HEDGE_MODE in ("gamma", "theta") else f"tol={tolerance:.1f}"
            self.Log(f"  [{self.Time.date()}] [{ticker}] HEDGE adj={adj:+d} "
                     f"stk=${s_price:.2f} K=${strike:.0f} "
                     f"delta={cur_delta:.4f} gamma={cur_gamma:.4f} "
                     f"theta={cur_theta:.2f} IV={cur_iv:.1%} "
                     f"posDelta={position_delta:.1f} {_tol_s} "
                     f"stk_qty={ts['stock_qty']}→{target}")

        self._filling_ticker = ticker
        self.MarketOrder(ts["stock_symbol"], adj)
        self._filling_ticker = None

        ts["hedge_count"] += 1
        ts["last_hedge_price"] = s_price

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _expiry_near_earnings(self, ticker, expiry):
        """True if expiry falls within 0..DAYS_AFTER_EARNINGS days after any earnings date."""
        ed = expiry.date() if hasattr(expiry, 'date') and callable(expiry.date) else expiry
        for earn in self._ts[ticker]["earnings_dates"]:
            diff = (ed - earn).days
            if 0 <= diff <= DAYS_AFTER_EARNINGS:
                return True
        return False

    def _count_trading_days(self, ticker, from_date, to_date):
        """Count trading days between from_date and to_date (inclusive of to_date)."""
        exch = self.Securities[self._ts[ticker]["stock_symbol"]].Exchange
        d = from_date
        count = 0
        while d < to_date:
            d += timedelta(days=1)
            if exch.Hours.IsDateOpen(d):
                count += 1
        return count

    def _select_put(self, chain, today, stock_price, ticker):
        """Select ATM put with expiry closest to K calendar days from today.
        If its IV is > IV_ROLLBACK_RATIO times the prior-week expiry IV,
        roll back to the prior-week put instead."""
        target_expiry = today + timedelta(days=K)
        window_lo = today + timedelta(days=max(K - 3, 1))
        window_hi = today + timedelta(days=K + 10)

        all_puts = [c for c in chain
                    if c.Right == OptionRight.Put]
        puts = [p for p in all_puts if window_lo <= p.Expiry.date() <= window_hi]
        if not puts:
            return None

        best_expiry = min(set(p.Expiry for p in puts),
                          key=lambda e: abs((e.date() - target_expiry).days))
        candidate = self._pick_atm(puts, best_expiry, stock_price)
        if candidate is None:
            return None

        # Compare IV to prior-week expiry; roll back if IV spike detected
        try:
            cand_iv = candidate.ImpliedVolatility
        except Exception:
            cand_iv = 0.0

        if cand_iv > 0 and IV_ROLLBACK_RATIO > 0:
            prior_target = best_expiry.date() - timedelta(days=7)
            prior_expiry_puts = [p for p in all_puts
                                 if p.Expiry.date() < best_expiry.date()]
            if prior_expiry_puts:
                # Find the closest expiry to 1 week before the candidate
                prior_expiry = min(set(p.Expiry for p in prior_expiry_puts),
                                   key=lambda e: abs((e.date() - prior_target).days))
                # Pick closest ATM/OTM strike at that expiry
                prior_put = self._pick_atm(prior_expiry_puts, prior_expiry, stock_price)
                if prior_put is not None:
                    # Pin the prior-week contract so it stays available
                    _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
                    self.AddOptionContract(prior_put.Symbol, _res)

                    try:
                        prior_iv = prior_put.ImpliedVolatility
                    except Exception:
                        prior_iv = 0.0

                    if prior_iv > 0 and cand_iv / prior_iv > IV_ROLLBACK_RATIO:
                        return prior_put

        return candidate

    def _pick_atm(self, puts, expiry, stock_price):
        """From a list of puts, pick the highest-strike OTM/ATM put at the given expiry.
        Returns None if all available strikes are ITM."""
        by_expiry = [p for p in puts if p.Expiry == expiry]
        if not by_expiry:
            return None
        otm_or_atm = [p for p in by_expiry if p.Strike <= stock_price]
        if not otm_or_atm:
            return None
        return max(otm_or_atm, key=lambda x: x.Strike)

    def _calc_realized_vol(self, symbol, lookback_days=30):
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

    def _get_vix(self):
        """Fetch most recent VIX close via History() with 5-day lookback."""
        try:
            h = self.History(self._vix_symbol, 5, Resolution.Daily)
            if not h.empty:
                return float(h["close"].iloc[-1])
        except Exception:
            pass
        return None

    # ── Emergency exit (stock split / auto-liquidation) ───────────────────────

    def _emergency_exit(self, ticker, reason):
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE" or ts.get("force_exited"):
            return
        ts["force_exited"] = True
        ts["state"]        = "EMERGENCY_PENDING"
        self.Log(f"  [{self.Time.date()}] EMERGENCY EXIT ({reason}): ticker={ticker}")

    def _finalize_emergency_exit(self, ticker):
        ts = self._ts[ticker]
        if ts["state"] != "EMERGENCY_PENDING":
            return

        self.Log(f"  [{self.Time.date()}] FINALIZING EMERGENCY EXIT for {ticker}")

        n_contracts = ts["put_contracts"]
        if n_contracts > 0 and ts.get("put_symbol") is not None:
            self._filling_ticker = ticker
            self.MarketOrder(ts["put_symbol"], n_contracts)
            self._filling_ticker = None

        actual_stock_qty = 0
        if self.Portfolio.ContainsKey(ts["stock_symbol"]):
            actual_stock_qty = self.Portfolio[ts["stock_symbol"]].Quantity
        if actual_stock_qty != 0:
            self._filling_ticker = ticker
            self.MarketOrder(ts["stock_symbol"], -actual_stock_qty)
            self._filling_ticker = None

        n_contracts = ts["put_contracts"]
        put_pnl = (ts["put_entry_fill"] - ts["put_exit_fill"]) * n_contracts * 100
        stk_pnl = ts["stock_realized"]
        total   = put_pnl + stk_pnl

        held_days = (self.Time.date() - ts["entry_date"]).days if ts["entry_date"] else 0

        ts["trade_log"].append({
            "entry_date":       ts["entry_date"],
            "exit_date":        self.Time.date(),
            "held_days":        held_days,
            "put_pnl":          put_pnl,
            "put_price_entry":  ts["put_entry_fill"],
            "put_price_exit":   ts["put_exit_fill"],
            "stk_pnl":          stk_pnl,
            "stk_chg_pct":      0.0,
            "total":            total,
            "iv_entry":         ts["put_entry_iv"],
            "iv_exit":          0.0,
            "iv_trade_min":     ts["iv_trade_min"] or 0.0,
            "iv_trade_max":     ts["iv_trade_max"] or 0.0,
            "iv_rank_entry":    ts["entry_iv_rank"],
            "rv":               ts["entry_rv"],
            "iv_hist_min":      ts["iv_hist_min"],
            "iv_hist_max":      ts["iv_hist_max"],
            "put_spread_entry": ts["put_spread_entry"],
            "put_spread_exit":  0,
            "vix_entry":        ts["vix_entry"],
            "vix_exit":         ts["vix_exit"],
            "entry_iv_pctl":    ts["entry_iv_pctl"],
            "exit_iv_pctl":     ts["exit_iv_pctl"],
            "hedge_count":      ts["hedge_count"],
        })
        self._reset(ticker)

    def _reset(self, ticker):
        ts = self._ts[ticker]
        ts["state"]             = "FLAT"
        ts["put_symbol"]        = None
        ts["put_contracts"]     = 0
        ts["put_entry_fill"]    = 0.0
        ts["put_exit_fill"]     = 0.0
        ts["put_entry_iv"]      = 0.0
        ts["stock_qty"]         = 0
        ts["stock_cost_basis"]  = 0.0
        ts["stock_realized"]    = 0.0
        ts["last_hedge_price"]  = 0.0
        ts["stock_entry_price"] = 0.0
        ts["entry_date"]        = None
        ts["entry_iv_rank"]     = 0.0
        ts["entry_rv"]          = 0.0
        ts["iv_hist_min"]       = 0.0
        ts["iv_hist_max"]       = 0.0
        ts["iv_trade_min"]      = None
        ts["iv_trade_max"]      = None
        ts["force_exited"]      = False
        ts["hedge_count"]       = 0
        ts["put_spread_entry"]  = 0
        ts["put_spread_exit"]   = 0
        ts["vix_entry"]         = None
        ts["vix_exit"]          = None
        ts["entry_iv_pctl"]     = None
        ts["exit_iv_pctl"]      = None

    # ── End-of-backtest summary ───────────────────────────────────────────────

    def _ol(self, lines, msg):
        self.Log(msg)
        lines.append(msg)

    def OnEndOfAlgorithm(self):
        lines = []
        grand_total  = 0.0
        grand_trades = 0
        grand_wins   = 0

        for ticker in self._ts:
            trade_log = self._ts[ticker]["trade_log"]
            n = len(trade_log)

            self._ol(lines, f"{'=' * 90}")
            if n == 0:
                self._ol(lines, f"  {ticker} SUMMARY  |  No trades completed")
                self._ol(lines, f"{'=' * 90}")
                continue

            totals = {"put": 0.0, "stk": 0.0, "total": 0.0}
            wins   = sum(1 for t in trade_log if t["total"] >= 0)
            grand_trades += n
            grand_wins   += wins

            self._ol(lines, f"  {ticker} SUMMARY  |  {n} trade(s)  |  Wins: {wins}/{n}")
            self._ol(lines, f"{'-' * 90}")

            for t in trade_log:
                tag = "[+]" if t["total"] >= 0 else "[-]"
                rv  = t.get("rv", 0.0)
                ratio = f"{t['iv_entry'] / rv:.2f}" if rv > 0 else "n/a"
                chg = t.get("stk_chg_pct", 0.0)
                _pse = t.get("put_spread_entry", 0)
                _psx = t.get("put_spread_exit", 0)
                _tmin = t.get("iv_trade_min", 0.0)
                _tmax = t.get("iv_trade_max", 0.0)
                _hmin = t.get("iv_hist_min", 0.0)
                _hmax = t.get("iv_hist_max", 0.0)
                D = "$"
                _pen = t.get("put_price_entry", 0.0)
                _pex = t.get("put_price_exit", 0.0)
                _vixen = t.get("vix_entry")
                _vixxs = t.get("vix_exit")
                _vixen_s = f"{_vixen:.1f}" if _vixen else "n/a"
                _vixxs_s = f"{_vixxs:.1f}" if _vixxs else "n/a"
                _pivEn = t.get("entry_iv_pctl")
                _pivEx = t.get("exit_iv_pctl")
                _pivEn_s = f"{_pivEn:.0f}" if _pivEn is not None else "n/a"
                _pivEx_s = f"{_pivEx:.0f}" if _pivEx is not None else "n/a"
                _hcnt = t.get("hedge_count", 0)
                self._ol(lines,
                    f"  {tag} {t['entry_date']!s:<11} {t['exit_date']!s:<11} days={t['held_days']}"
                    f"  PutPnL={D}{t['put_pnl']:>+,.0f}"
                    f"  PrEn={_pen:.2f} PrEx={_pex:.2f}"
                    f"  StkPnL={D}{t['stk_pnl']:>+,.0f}"
                    f"  Stk={chg:>+.1f}%"
                    f"  PnL={D}{t['total']:>+,.0f}"
                    f"  IVR={t['iv_rank_entry']:.0f} IV/RV={ratio}"
                    f"  Pctl={_pivEn_s:>4} PctlEx={_pivEx_s:>4}"
                    f"  IVen={t['iv_entry']:.1%} IVex={t['iv_exit']:.1%}"
                    f"  VIXen={_vixen_s:>6} VIXex={_vixxs_s:>6}"
                    f"  TrMin={_tmin:.1%} TrMax={_tmax:.1%}"
                    f"  HMin={_hmin:.1%} HMax={_hmax:.1%}"
                    f"  SpEn={_pse} SpEx={_psx}"
                    f"  Hdg={_hcnt}"
                )
                totals["put"]   += t["put_pnl"]
                totals["stk"]   += t["stk_pnl"]
                totals["total"] += t["total"]

            avg = totals["total"] / n if n > 0 else 0.0
            self._ol(lines, f"  {'-' * 90}")
            self._ol(lines,
                f"  {'TOTAL':<26} {'':>5}"
                f"  ${totals['put']:>+10,.2f}"
                f"  ${totals['stk']:>+10,.2f}"
                f"  {'':>8}"
                f"  ${totals['total']:>+10,.2f}"
            )
            self._ol(lines, f"  Avg PnL/trade: ${avg:+,.2f}")
            self._ol(lines, f"{'=' * 90}")
            grand_total += totals["total"]

        if len(self._ts) > 1:
            self._ol(lines, f"{'=' * 90}")
            wp = (grand_wins / grand_trades * 100) if grand_trades else 0
            avg_all = (grand_total / grand_trades) if grand_trades else 0
            self._ol(lines,
                f"  ALL TICKERS COMBINED  |  {grand_trades} trade(s)  |  "
                f"Wins: {grand_wins}/{grand_trades} ({wp:.1f}%)  |  "
                f"Max concurrent: {self._max_concurrent}")
            grand_iv_fails = sum(ts["iv_sample_fails"] for ts in self._ts.values())
            self._ol(lines, f"  Combined PnL: ${grand_total:+,.2f}  |  Avg PnL/trade: ${avg_all:+,.2f}")
            self._ol(lines, f"  IV sample fails: {grand_iv_fails}")
            self._ol(lines, f"{'=' * 90}")

        self.ObjectStore.Save("backtest_logs", "\n".join(lines))


# ─── Mid-price fill model ─────────────────────────────────────────────────────

class NullAssignmentModel(DefaultOptionAssignmentModel):
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
        _no_fill = getattr(OrderStatus, 'None')
        if order.Quantity > 0 and mid > order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid above limit")
        if order.Quantity < 0 and mid < order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid below limit")
        fill = super().LimitFill(asset, order)
        if fill.Status == OrderStatus.Filled:
            fill.FillPrice = round(mid, 2)
        return fill
