# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import math
from cal_config import *
import cal_exit_handlers as _h
import cal_greeks as _g
import cal_helpers as _u
from cal_greeks import bs_put_greeks, IVSmoother
# endregion


class EarningsCalendarPutMultiTicker(QCAlgorithm):
    # Exit / emergency handlers
    _immediate_close_all      = _h._immediate_close_all
    _finalize_forced_exit     = _h._finalize_forced_exit
    _emergency_exit           = _h._emergency_exit
    _finalize_emergency_exit  = _h._finalize_emergency_exit
    _check_orphaned_positions = _h._check_orphaned_positions
    _finalize_exit            = _h._finalize_exit
    # Greeks helpers (custom Black-Scholes path + IV smoothing)
    _get_risk_free_rate       = _g._get_risk_free_rate
    _get_dividend_yield       = _g._get_dividend_yield
    _sample_iv_for_smoothers  = _g._sample_iv_for_smoothers
    # Utility helpers (earnings loading, RV, VIX, trading-day math, state reset)
    _load_earnings_dates      = _u._load_earnings_dates
    _log_earnings_dates       = _u._log_earnings_dates
    _calc_realized_vol        = _u._calc_realized_vol
    _get_vix                  = _u._get_vix
    _offset_trading_days      = _u._offset_trading_days
    _reset                    = _u._reset
    _log                      = _u._log
    _ol                       = _u._ol
    OnEndOfAlgorithm          = _u.OnEndOfAlgorithm
    """
    
    """

    # ── Initialise ────────────────────────────────────────────────────────────

    def Initialize(self):
        self.SetStartDate(2020, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(5_000_000)

        tickers = list(MANUAL_EARNINGS_DATES.keys())

        # Per-ticker state dict
        self._ts = {}
        self._max_concurrent = 0   # peak number of tickers held simultaneously
        self._all_lines = []       # collect ALL log lines for ObjectStore
        self._filling_ticker = None  # set around our own orders to distinguish from auto-liquidation
        self._total_fees = 0.0     # accumulated fees across all tickers and orders
        self._theta_exits = 0      # count of trades closed by THETA_WATCHER
        self._vix_symbol = self.AddData(CBOE, "VIX", Resolution.Daily).Symbol

        # ── Custom-greeks support (see cal_greeks.py + cal_config.py) ──
        # Risk-free rate model handle (used by _get_risk_free_rate)
        try:
            self._risk_free_model = self.RiskFreeInterestRateModel
        except Exception:
            self._risk_free_model = None
        # Per-ticker dividend-yield cache: ticker → (yield, last_refresh_date)
        self._dividend_yields = {}

        _exp_max = K * 2 + 20   # broad enough to capture weeklies around earnings

        for ticker in tickers:
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            eq  = self.AddEquity(ticker, _res)
            opt = self.AddOption(ticker, _res)
            opt.SetFilter(lambda u, e=_exp_max: u.Strikes(-5, 0).Expiration(0, e).PutsOnly())
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
                "state":                "FLAT",
                "put_symbol":           None,     # long put (buy)
                "short_put_symbol":     None,     # short put (sell)
                "call_symbol_long":    None,     # call at long put strike/expiry (for sim PnL)
                "call_symbol_short":   None,     # call at short put strike/expiry (for sim PnL)
                "put_contracts":        0,        # same qty for both legs
                "put_entry_fill":       0.0,      # long put entry fill
                "put_exit_fill":        0.0,      # long put exit fill
                "short_put_entry_fill": 0.0,      # short put entry fill
                "short_put_exit_fill":  0.0,      # short put exit fill
                "short_put_expiry":     None,     # date — drives exit timing
                "put_entry_iv":         0.0,      # IV of long put at entry
                "short_put_entry_iv":   0.0,      # IV of short put at entry
                "put_order_ticket":     None,     # limit order ticket for long put
                "short_put_order_ticket": None,   # limit order ticket for short put (entry)
                "long_exit_ticket":     None,     # limit order ticket for long put (exit)
                "short_exit_ticket":    None,     # limit order ticket for short put (exit)
                "pending_short_put":    False,    # True until short put placed after long put fills
                "pending_hedge_shares": 0,
                "entry_aborted":        False,    # True if short never filled at mid → unwinding long
                "exit_n_contracts":     0,        # snapshot for _finalize_exit
                "exit_n_shares":        0,        # snapshot for _finalize_exit
                "exit_put_iv":          0.0,      # snapshot for _finalize_exit
                "stock_qty":            0,
                "stock_cost_basis":     0.0,
                "stock_realized":       0.0,
                "last_hedge_price":     0.0,
                "stock_max_up_pct":    0.0,
                "stock_max_dn_pct":    0.0,
                "entry_earnings":       None,
                "entry_rv":             0.0,
                "vix_entry":           None,
                "vix_exit":            None,
                # chain cache
                "chain": None,
                # data
                "earnings_dates":  dates,
                "traded_earnings": set(),
                "trade_log":       [],
                "hedge_count":     0,
                "force_exited":    False,
                "_closing_forced": False,   # True while a forced-exit function is actively closing legs
                "orphan_cleaned":  False,   # True after FLAT-state orphan stock sold (prevent repeats)
                "total_fees":      0.0,     # accumulated order fees for this ticker cycle
                # ── Bid-ask spread cost tracking ──
                "long_spread_entry":  0,
                "short_spread_entry": 0,
                "long_spread_exit":   0,
                "short_spread_exit":  0,
                # ── Skip counters (cumulative across entire backtest) ──
                "entry_attempts":     0,    # times _enter_position was called
                "skips_no_pair":      0,    # _select_calendar_puts returned None (no weeklies / spread too wide)
                "skips_low_debit":    0,    # net_debit <= 0 or < MIN_NET_DEBIT
                "skips_other":        0,    # chain missing, bad price, MAX_PUT_PCT, IV/RV filter
                # ── Custom-greeks IV smoothers (None until trade activation) ──
                "long_iv_smoother":   None,
                "short_iv_smoother":  None,
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
            self.Schedule.On(
                self.DateRules.EveryDay(ticker),
                self.TimeRules.Every(timedelta(hours=1)),
                lambda t=ticker: self._check_orphaned_positions(t),
            )

    # ── Fill model ────────────────────────────────────────────────────────────

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFillModel(MidPriceFillModel())

    # ── Data handler ──────────────────────────────────────────────────────────

    def OnData(self, data):
        for ticker, ts in self._ts.items():
            if ts["option_symbol"] in data.OptionChains:
                ts["chain"] = data.OptionChains[ts["option_symbol"]]

                # ── Bar-level IV sampling for custom-greeks smoothers ──
                # Runs every bar an ACTIVE position has a refreshed chain so
                # the rolling 5-day IV averages have ~390 samples/day to work
                # with (minute resolution). Long & short legs are smoothed
                # in fully separate buffers — never mingled.
                if (COMPUTE_OWN_GREEKS
                        and ts["state"] == "ACTIVE"
                        and ts.get("long_iv_smoother") is not None):
                    self._sample_iv_for_smoothers(ticker, ts)

            # ── Stock split detection ───────────────────────────────────
            if data.Splits.ContainsKey(ts["stock_symbol"]):
                split = data.Splits[ts["stock_symbol"]]
                if split.Type == SplitType.SplitOccurred:
                    if ts["state"] in ("ACTIVE", "EMERGENCY_PENDING"):
                        if ts["stock_qty"] != 0 and split.SplitFactor > 0:
                            ts["stock_qty"] = round(ts["stock_qty"] / split.SplitFactor)
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
                is_our_put = (symbol == ts.get("put_symbol") or
                              symbol == ts.get("short_put_symbol"))
                if is_our_put and self._filling_ticker != ticker:
                    order = self.Transactions.GetOrderById(orderEvent.OrderId)
                    if order is not None and order.Type == OrderType.MarketOnClose:
                        ts["force_exited"] = True
                        self._log(f"  [{self.Time.date()}] SPLIT/DELISTING MOC submitted "
                                  f"for {ticker} option — closing all legs immediately")
                        self._filling_ticker = ticker
                        # Close long put
                        if ts.get("put_symbol") and symbol != ts.get("put_symbol"):
                            try:
                                lq = self.Portfolio[ts["put_symbol"]].Quantity \
                                     if self.Portfolio.ContainsKey(ts["put_symbol"]) else 0
                            except Exception:
                                lq = 0
                            if lq != 0:
                                self.MarketOrder(ts["put_symbol"], -lq)
                        # Close short put
                        if ts.get("short_put_symbol") and symbol != ts.get("short_put_symbol"):
                            try:
                                sq = self.Portfolio[ts["short_put_symbol"]].Quantity \
                                     if self.Portfolio.ContainsKey(ts["short_put_symbol"]) else 0
                            except Exception:
                                sq = 0
                            if sq != 0:
                                self.MarketOrder(ts["short_put_symbol"], -sq)
                        # Close stock
                        try:
                            stk_qty = self.Portfolio[ts["stock_symbol"]].Quantity \
                                      if self.Portfolio.ContainsKey(ts["stock_symbol"]) else 0
                        except Exception:
                            stk_qty = 0
                        if stk_qty != 0:
                            self.MarketOrder(ts["stock_symbol"], -stk_qty)
                        self._filling_ticker = None
            return

        if orderEvent.Status != OrderStatus.Filled:
            return

        # ── Accumulate fees globally (QC chart deducts these) ──────────
        try:
            fee_amount = orderEvent.OrderFee.Value.Amount
            self._total_fees += fee_amount
        except Exception:
            pass

        symbol     = orderEvent.Symbol
        fill_price = orderEvent.FillPrice
        fill_qty   = orderEvent.FillQuantity   # +buy / −sell

        for ticker, ts in self._ts.items():
            if ts["state"] not in ("ACTIVE", "EMERGENCY_PENDING", "EXITING"):
                continue

            # ── Long put fill ────────────────────────────────────────
            if symbol == ts.get("put_symbol"):
                if fill_qty > 0:
                    ts["put_entry_fill"] = fill_price
                    # Deferred: sell short put now that long put is filled
                    if ts.get("pending_short_put"):
                        short_sec = self.Securities[ts["short_put_symbol"]]
                        short_mid_now = _mid(short_sec.BidPrice, short_sec.AskPrice)
                        if short_mid_now <= 0:
                            # Degenerate short quote → ABORT entry: unwind the long we just bought
                            self._log(f"  [{ticker}] ABORT entry: short_put quote degenerate "
                                      f"(bid={short_sec.BidPrice:.2f} ask={short_sec.AskPrice:.2f}) "
                                      f"— unwinding long put")
                            ts["entry_aborted"] = True
                            ts["pending_short_put"]    = False
                            ts["pending_hedge_shares"] = 0
                            self._filling_ticker = ticker
                            self.MarketOrder(ts["put_symbol"], -ts["put_contracts"])
                            self._filling_ticker = None
                            # _reset() will run when the unwind sell-fill comes back below
                        else:
                            short_limit_px = round(short_mid_now * SHORT_PUT_LIMIT_MULT, 2)
                            self._filling_ticker = ticker
                            st_ticket = self.LimitOrder(
                                ts["short_put_symbol"], -ts["put_contracts"], short_limit_px)
                            self._filling_ticker = None
                            ts["short_put_order_ticket"] = st_ticket
                            ts["pending_short_put"] = False
                    # NOTE: stock hedge is now deferred to the short-fill branch below,
                    # so both put legs are open before we hedge delta.
                else:
                    ts["put_exit_fill"] = fill_price
                    if ts.get("force_exited"):
                        if not ts.get("_closing_forced"):
                            self._finalize_forced_exit(ticker)
                        return
                    # Entry-unwind sell (short never filled at mid) → just reset
                    if ts.get("entry_aborted"):
                        self._log(f"  [{ticker}] Entry-unwind long put sold at "
                                  f"${fill_price:.2f} — resetting (no trade-log row)")
                        self._reset(ticker)
                        return
                    # During our own EXITING flow → record fill, no warning
                    if ts["state"] == "EXITING":
                        return
                    # Detect auto-liquidation: long put sold but NOT by our code
                    if self._filling_ticker != ticker:
                        self._log(f"  [{self.Time.date()}] AUTO-LIQUIDATION detected: "
                                 f"long put {symbol} sold at ${fill_price:.2f} (likely stock split)")
                        self._immediate_close_all(ticker, "long put auto-liquidation")
                return

            # ── Short put fill ───────────────────────────────────────
            if symbol == ts.get("short_put_symbol"):
                if fill_qty < 0:
                    # We sold the short put (entry)
                    ts["short_put_entry_fill"] = fill_price
                    # Deferred: place stock hedge now that BOTH put legs are open
                    pending = ts.get("pending_hedge_shares", 0)
                    if pending != 0:
                        self._filling_ticker = ticker
                        self.MarketOrder(ts["stock_symbol"], pending)
                        self._filling_ticker = None
                        ts["pending_hedge_shares"] = 0
                else:
                    # We bought back the short put (exit) — likely ASSIGNMENT
                    ts["short_put_exit_fill"] = fill_price
                    if ts.get("force_exited"):
                        if not ts.get("_closing_forced"):
                            self._finalize_forced_exit(ticker)
                        return
                    # During our own EXITING flow → record fill, no warning
                    if ts["state"] == "EXITING":
                        return
                    # Detect assignment / auto-liquidation of short put
                    if self._filling_ticker != ticker:
                        self._log(f"  [{self.Time.date()}] ASSIGNMENT detected: "
                                 f"short put {symbol} bought at ${fill_price:.2f}")
                        self._immediate_close_all(ticker, "short put assignment")
                return

            # ── Stock fill (symmetric long / short tracking) ──────────
            if symbol == ts["stock_symbol"]:
                old_qty = ts["stock_qty"]

                if fill_qty > 0:                       # ── BUY ──
                    if old_qty < 0:
                        # Covering (part of) a short position
                        coverable = min(fill_qty, abs(old_qty))
                        avg_short = (ts["stock_cost_basis"] / old_qty
                                     if old_qty != 0 else fill_price)
                        ts["stock_realized"]   += (avg_short - fill_price) * coverable
                        ts["stock_cost_basis"] += avg_short * coverable   # reduce negative basis
                        ts["stock_qty"]        += coverable
                        leftover = fill_qty - coverable
                        if leftover > 0:               # flip to long
                            ts["stock_cost_basis"] += leftover * fill_price
                            ts["stock_qty"]        += leftover
                    else:
                        # Adding to / opening a long position
                        ts["stock_cost_basis"] += fill_qty * fill_price
                        ts["stock_qty"]        += fill_qty

                else:                                  # ── SELL ──
                    sold = abs(fill_qty)
                    if old_qty > 0:
                        # Closing (part of) a long position
                        closable = min(sold, old_qty)
                        avg_cost = (ts["stock_cost_basis"] / old_qty
                                    if old_qty != 0 else fill_price)
                        ts["stock_realized"]   += (fill_price - avg_cost) * closable
                        ts["stock_cost_basis"] -= avg_cost * closable
                        ts["stock_qty"]        -= closable
                        leftover = sold - closable
                        if leftover > 0:               # flip to short
                            ts["stock_cost_basis"] -= leftover * fill_price
                            ts["stock_qty"]        -= leftover
                    else:
                        # Opening / adding to a short position
                        ts["stock_cost_basis"] -= sold * fill_price
                        ts["stock_qty"]        -= sold
                return

    # ── Position management (entry / exit at TRADE_TIME_MIN) ─────────────────

    def _manage_position(self, ticker):
        ts = self._ts[ticker]

        # ── Finalize any emergency exit from a stock split ──────────
        if ts["state"] == "EMERGENCY_PENDING":
            self._finalize_emergency_exit(ticker)
            return

        if not ts["earnings_dates"]:
            return

        today = self.Time.date()

        # ── Underlying-tradable guard ────────────────────────────────
        # If the stock has been delisted mid-trade (acquisition close,
        # reverse merger, exchange suspension, etc.), QC throws on
        # AddOptionContract / chain lookups for that symbol. Rather than
        # crash, mark the position force_exited and defer finalization to
        # QC's natural MOC auto-liquidation, which OnOrderEvent's
        # SUBMITTED handler (lines 190-232) already catches and converts
        # into a proper trade-log row via _finalize_forced_exit.
        stock_sym = ts.get("stock_symbol")
        stock_sec = (self.Securities[stock_sym]
                     if stock_sym and self.Securities.ContainsKey(stock_sym)
                     else None)
        if stock_sec is None or not stock_sec.IsTradable:
            if ts["state"] == "ACTIVE" and not ts.get("force_exited"):
                ts["force_exited"] = True
                self._log(f"  [{self.Time.date()}] [{ticker}] underlying "
                          f"delisted/untradable — deferring to QC auto-"
                          f"liquidation MOC for finalization")
            return

        # Re-pin all four contracts daily — guards against QC universe drops
        # mid-trade (esp. the calls used for sim_pnl at exit via put-call parity).
        if ts["state"] == "ACTIVE":
            _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
            for _key in ("put_symbol", "short_put_symbol",
                         "call_symbol_long", "call_symbol_short"):
                _sym = ts.get(_key)
                if _sym is not None:
                    try:
                        self.AddOptionContract(_sym, _res)
                    except Exception as e:
                        # Belt-and-suspenders: if the underlying gets delisted
                        # between the IsTradable check above and this call
                        # (or AddOptionContract throws for any other reason),
                        # treat it the same — force-exit + defer to QC MOC.
                        if not ts.get("force_exited"):
                            ts["force_exited"] = True
                            self._log(f"  [{self.Time.date()}] [{ticker}] "
                                      f"AddOptionContract failed for {_key}: "
                                      f"{e} — deferring to QC auto-liquidation")
                        return

        # ── Check exit: EXIT_DAYS_BEFORE trading days before short put expiry ──
        if ts["state"] == "ACTIVE" and ts.get("short_put_expiry"):
            exit_day = self._offset_trading_days(ticker, ts["short_put_expiry"], -EXIT_DAYS_BEFORE)
            if today == exit_day:
                self._exit_position(ticker)
                return

        # ── Check entry ─────────────────────────────────────────────
        for ed_dt in reversed(ts["earnings_dates"]):
            ed = ed_dt.date() if isinstance(ed_dt, datetime) else ed_dt
            if ed in ts["traded_earnings"]:
                continue

            entry_day = self._offset_trading_days(ticker, ed, -K)

            if ts["state"] == "FLAT" and today == entry_day:
                self._enter_position(ticker, ed_dt)
                break

    # ── Calendar put selection ─────────────────────────────────────────────────

    def _select_calendar_puts(self, chain, earnings_date, stock_price):
        """Select a calendar put pair: same strike, short expiry before
        earnings and long expiry after earnings.

        Short put: closest expiry before earnings (must be within MAX_SHORT_EARN_DAYS).
        Long put:  the N_WEEKLY_AFTER_EARNINGS-th weekly expiry on or after earnings.

        Tries all strikes from ATM downward (highest <= stock_price first).
        Returns ((long_put, short_put), "") or (None, reason_string).
        """
        puts = [c for c in chain if c.Right == OptionRight.Put]
        if not puts:
            return (None, "no puts in chain")

        valid_strikes = sorted(set(p.Strike for p in puts if p.Strike <= stock_price),
                               reverse=True)
        if not valid_strikes:
            return (None, f"no strikes <= ${stock_price:.2f}")

        first_reason = ""

        for strike in valid_strikes:
            strike_puts = [p for p in puts if p.Strike == strike]
            if len(strike_puts) < 2:
                if not first_reason:
                    first_reason = f"K={strike} only {len(strike_puts)} expiry"
                continue

            before = [p for p in strike_puts if p.Expiry.date() < earnings_date]
            after  = [p for p in strike_puts if p.Expiry.date() >= earnings_date]

            if not before or not after:
                if not first_reason:
                    expiries = sorted(set(p.Expiry.date() for p in strike_puts))
                    first_reason = (f"K={strike} before={len(before)} after={len(after)} "
                                    f"expiries={expiries} earn={earnings_date}")
                continue

            short_put = max(before, key=lambda p: p.Expiry)

            gap_days = (earnings_date - short_put.Expiry.date()).days
            if gap_days > MAX_SHORT_EARN_DAYS:
                if not first_reason:
                    first_reason = (f"K={strike} short_exp={short_put.Expiry.date()} "
                                    f"gap={gap_days}d > {MAX_SHORT_EARN_DAYS}d to earn={earnings_date}")
                continue

            after_sorted = sorted(after, key=lambda p: p.Expiry)
            n_idx = N_WEEKLY_AFTER_EARNINGS - 1
            if n_idx >= len(after_sorted):
                if not first_reason:
                    first_reason = (f"K={strike} only {len(after_sorted)} expiry(s) after earnings, "
                                    f"need {N_WEEKLY_AFTER_EARNINGS}")
                continue
            long_put = after_sorted[n_idx]

            return ((long_put, short_put), "")

        return (None, f"{len(valid_strikes)} strikes tried, first fail: {first_reason}")

    # ── Entry ──────────────────────────────────────────────────────────────────

    def _enter_position(self, ticker, earnings_dt):
        ts  = self._ts[ticker]
        ed  = earnings_dt.date() if isinstance(earnings_dt, datetime) else earnings_dt

        ts["entry_attempts"] += 1

        if ts["chain"] is None:
            ts["skips_other"] += 1
            return

        ts["orphan_cleaned"] = False   # allow orphan cleanup for next cycle

        # ── Safety: close any orphaned stock from a prior assignment ─────
        try:
            pre_stk = self.Portfolio[ts["stock_symbol"]].Quantity \
                      if self.Portfolio.ContainsKey(ts["stock_symbol"]) else 0
        except Exception:
            pre_stk = 0
        if pre_stk != 0:
            self._log(f"  [{ticker}] PRE-ENTRY: closing {pre_stk} orphaned shares "
                     f"before new trade")
            self._filling_ticker = ticker
            self.MarketOrder(ts["stock_symbol"], -pre_stk)
            self._filling_ticker = None

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            ts["skips_other"] += 1
            return

        # ── Min-stock-price filter ────────────────────────────────────────
        if MIN_STOCK_PRICE > 0 and s_price < MIN_STOCK_PRICE:
            self._log(f"  [{ticker}] SKIP — stock price ${s_price:.2f} < "
                     f"min ${MIN_STOCK_PRICE:.2f}")
            ts["skips_other"] += 1
            return

        # ── Market-cap filter ─────────────────────────────────────────────
        if MIN_MARKET_CAP > 0:
            mcap = 0.0
            try:
                if stock.Fundamentals is not None:
                    mcap = stock.Fundamentals.MarketCap or 0.0
            except Exception:
                mcap = 0.0
            if mcap > 0 and mcap < MIN_MARKET_CAP:
                self._log(f"  [{ticker}] SKIP — market cap ${mcap:,.0f} < "
                         f"min ${MIN_MARKET_CAP:,.0f}")
                ts["skips_other"] += 1
                return

        result, skip_reason = self._select_calendar_puts(ts["chain"], ed, s_price)
        if result is None:
            self._log(f"  [{ticker}] SKIP no_pair for earnings {ed}: {skip_reason}")
            ts["skips_no_pair"] += 1
            return

        long_put, short_put = result

        long_mid  = _mid(long_put.BidPrice,  long_put.AskPrice)
        short_mid = _mid(short_put.BidPrice, short_put.AskPrice)
        if long_mid <= 0 or short_mid <= 0:
            ts["skips_other"] += 1
            return

        net_debit = long_mid - short_mid
        if net_debit <= 0:
            self._log(f"  [{ticker}] SKIP — net debit <= 0 "
                     f"(long={long_mid:.2f}, short={short_mid:.2f})")
            ts["skips_low_debit"] += 1
            return
        if net_debit < MIN_NET_DEBIT:
            self._log(f"  [{ticker}] SKIP — net debit ${net_debit:.2f} < min ${MIN_NET_DEBIT:.2f}")
            ts["skips_low_debit"] += 1
            return

        # Sanity check: skip if long put price unreasonably high vs underlying
        if MAX_PUT_PCT > 0 and long_mid > s_price * MAX_PUT_PCT:
            self._log(f"  [{ticker}] SKIP — long_put_mid ${long_mid:.2f} > "
                     f"{MAX_PUT_PCT:.0%} of stock ${s_price:.2f}")
            ts["skips_other"] += 1
            return

        n_contracts = max(1, int(S / (net_debit * 100)))

        # ── Realized volatility + IV/RV filter ───────────────────────────────
        rv = self._calc_realized_vol(ts["stock_symbol"], 30)

        try:
            cur_iv = long_put.ImpliedVolatility
        except Exception:
            cur_iv = 0.0

        try:
            short_iv = short_put.ImpliedVolatility
        except Exception:
            short_iv = 0.0

        if Z > 0 and rv > 0:
            if cur_iv / rv >= Z:
                ts["skips_other"] += 1
                return

        # Combined calendar delta for initial hedge
        long_delta  = long_put.Greeks.Delta    # negative (long put)
        short_delta = short_put.Greeks.Delta   # negative (short put)
        # Net option delta: long +n → long_delta*n*100, short −n → −short_delta*n*100
        net_option_delta = (long_delta - short_delta) * n_contracts * 100
        # Stock to offset: buy if net_option_delta < 0, sell if > 0
        n_shares = round(-net_option_delta)

        # Activate BEFORE placing orders so OnOrderEvent can track fills
        ts["state"]               = "ACTIVE"
        ts["put_symbol"]          = long_put.Symbol
        ts["short_put_symbol"]    = short_put.Symbol
        ts["put_contracts"]       = n_contracts
        ts["put_entry_fill"]      = 0.0
        ts["put_exit_fill"]       = 0.0
        ts["short_put_entry_fill"]= 0.0
        ts["short_put_exit_fill"] = 0.0
        ts["short_put_expiry"]    = short_put.Expiry.date()
        ts["put_entry_iv"]        = cur_iv
        ts["short_put_entry_iv"]  = short_iv
        ts["stock_qty"]           = 0
        ts["stock_cost_basis"]    = 0.0
        ts["stock_realized"]      = 0.0
        ts["last_hedge_price"]    = s_price
        ts["stock_entry_price"]   = s_price
        ts["stock_max_up_pct"]    = 0.0
        ts["stock_max_dn_pct"]    = 0.0
        ts["entry_earnings"]      = earnings_dt
        ts["entry_rv"]            = rv
        ts["long_spread_entry"]   = round(abs(long_put.BidPrice - long_put.AskPrice) * 100 * n_contracts)
        ts["short_spread_entry"]  = round(abs(short_put.BidPrice - short_put.AskPrice) * 100 * n_contracts)

        # ── Per-leg IV smoothers (used by _delta_hedge if COMPUTE_OWN_GREEKS=True) ──
        # Long and short legs have distinct IV regimes (long spans earnings → carries
        # event premium; short expires before earnings → no event premium). They are
        # smoothed in fully separate buffers and never mingled.
        if COMPUTE_OWN_GREEKS:
            ts["long_iv_smoother"]  = IVSmoother(window_days=IV_SMOOTH_DAYS)
            ts["short_iv_smoother"] = IVSmoother(window_days=IV_SMOOTH_DAYS)
            # Seed each buffer with its own entry IV (already validated by entry filters).
            ts["long_iv_smoother"].seed(cur_iv,    self.Time)
            ts["short_iv_smoother"].seed(short_iv, self.Time)

        # ── Pin both contracts so the universe filter can never unsubscribe them ──
        # Also set NullAssignmentModel on each contract individually — the
        # parent chain's model does NOT propagate to manually-added contracts.
        _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
        long_sec  = self.AddOptionContract(long_put.Symbol, _res)
        short_sec = self.AddOptionContract(short_put.Symbol, _res)
        long_sec.SetOptionAssignmentModel(NullAssignmentModel())
        short_sec.SetOptionAssignmentModel(NullAssignmentModel())

        # Subscribe to calls at both strikes/expiries (for sim PnL at exit via put-call parity)
        call_long = Symbol.CreateOption(
            ts["stock_symbol"], long_put.Symbol.ID.Market,
            OptionStyle.American, OptionRight.Call,
            long_put.Strike, long_put.Expiry)
        self.AddOptionContract(call_long, _res)
        ts["call_symbol_long"] = call_long

        call_short = Symbol.CreateOption(
            ts["stock_symbol"], short_put.Symbol.ID.Market,
            OptionStyle.American, OptionRight.Call,
            short_put.Strike, short_put.Expiry)
        self.AddOptionContract(call_short, _res)
        ts["call_symbol_short"] = call_short

        # Defer short put + stock hedge until long put limit order fills
        ts["pending_short_put"]    = True
        ts["pending_hedge_shares"] = n_shares
        limit_px = round(long_mid * PUT_LIMIT_MULT, 2)
        ticket = self.LimitOrder(long_put.Symbol, n_contracts, limit_px)
        ts["put_order_ticket"] = ticket

        iv_spread = cur_iv - short_iv
        short_iv_rv = short_iv / rv if rv > 0 else 0.0

        self._log(f"  [{ticker}] ENTRY: Long={long_put.Symbol} Short={short_put.Symbol} "
                 f"K={long_put.Strike} LongExp={long_put.Expiry.date()} "
                 f"ShortExp={short_put.Expiry.date()} "
                 f"Spread={(long_put.Expiry.date()-short_put.Expiry.date()).days}d "
                 f"LongMid={long_mid:.2f} ShortMid={short_mid:.2f} "
                 f"NetDebit={net_debit:.2f} n={n_contracts} "
                 f"LongIV={cur_iv:.1%} ShortIV={short_iv:.1%} "
                 f"IVspread={iv_spread:.1%} ShortIV/RV={short_iv_rv:.2f} "
                 f"ExitDate={self._offset_trading_days(ticker, short_put.Expiry.date(), -EXIT_DAYS_BEFORE)}")

        # Track peak concurrent positions
        active = sum(1 for t in self._ts.values() if t["state"] == "ACTIVE")
        if active > self._max_concurrent:
            self._max_concurrent = active

        ts["vix_entry"] = self._get_vix()

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _exit_position(self, ticker):
        """Request exit: places LIMIT orders at mid for both put legs (with
        market-order fallback for degenerate quotes), market order for stock,
        and sets state=EXITING. PnL computation + trade-log row are deferred
        to _finalize_exit(), called at the 3:45 PM hedge tick once any
        unfilled limits have been cancelled and market-flattened.
        """
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE":
            return
        if ts.get("force_exited"):
            return   # already emergency-closed (e.g. stock split)

        ts["vix_exit"] = self._get_vix()

        # Read exit IV from chain (long put, for logging + min/max tracking)
        put_exit_iv = 0.0
        if ts["chain"]:
            for c in ts["chain"]:
                if c.Symbol == ts["put_symbol"]:
                    try:
                        put_exit_iv = c.ImpliedVolatility
                    except Exception:
                        put_exit_iv = 0.0
                    break
        ts["exit_put_iv"] = put_exit_iv

        # Snapshot quantities before orders (OnOrderEvent will update them as fills arrive)
        n_contracts = ts["put_contracts"]
        n_shares    = ts["stock_qty"]
        ts["exit_n_contracts"] = n_contracts
        ts["exit_n_shares"]    = n_shares

        # Record exit bid-ask spread costs from current quotes
        try:
            _long_sec  = self.Securities[ts["put_symbol"]]
            ts["long_spread_exit"] = round(abs(_long_sec.BidPrice - _long_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            _long_sec = None
            ts["long_spread_exit"] = 0
        try:
            _short_sec = self.Securities[ts["short_put_symbol"]]
            ts["short_spread_exit"] = round(abs(_short_sec.BidPrice - _short_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            _short_sec = None
            ts["short_spread_exit"] = 0

        # ── Place LIMIT orders at mid for both put legs ────────────────────
        # Stock hedge stays as MarketOrder (liquid, MidPriceFillModel overrides to mid).
        # If a put quote is degenerate (mid <= 0), fall back to MarketOrder for that leg.
        self._filling_ticker = ticker
        ts["state"] = "EXITING"   # signals OnOrderEvent that fills are exits, not assignments

        if n_contracts > 0 and _long_sec is not None:
            long_mid_now = _mid(_long_sec.BidPrice, _long_sec.AskPrice)
            if long_mid_now > 0:
                long_limit_px = round(long_mid_now * EXIT_LIMIT_MULT_LONG, 2)
                lt = self.LimitOrder(ts["put_symbol"], -n_contracts, long_limit_px)
                ts["long_exit_ticket"] = lt
            else:
                self._log(f"  [{ticker}] EXIT: long_mid degenerate "
                          f"(bid={_long_sec.BidPrice:.2f} ask={_long_sec.AskPrice:.2f}) "
                          f"— falling back to MarketOrder for long put")
                self.MarketOrder(ts["put_symbol"], -n_contracts)
                ts["long_exit_ticket"] = None

            # Only buy back short put if we actually hold it (may already be assigned)
            short_qty = 0
            if self.Portfolio.ContainsKey(ts["short_put_symbol"]):
                short_qty = self.Portfolio[ts["short_put_symbol"]].Quantity
            if short_qty != 0 and _short_sec is not None:
                short_mid_now = _mid(_short_sec.BidPrice, _short_sec.AskPrice)
                if short_mid_now > 0:
                    short_limit_px = round(short_mid_now * EXIT_LIMIT_MULT_SHORT, 2)
                    st = self.LimitOrder(ts["short_put_symbol"], -short_qty, short_limit_px)
                    ts["short_exit_ticket"] = st
                else:
                    self._log(f"  [{ticker}] EXIT: short_mid degenerate "
                              f"(bid={_short_sec.BidPrice:.2f} ask={_short_sec.AskPrice:.2f}) "
                              f"— falling back to MarketOrder for short put")
                    self.MarketOrder(ts["short_put_symbol"], -short_qty)
                    ts["short_exit_ticket"] = None
            else:
                ts["short_exit_ticket"] = None
        else:
            ts["long_exit_ticket"]  = None
            ts["short_exit_ticket"] = None

        if n_shares != 0:
            self.MarketOrder(ts["stock_symbol"], -n_shares)
        self._filling_ticker = None

        self._log(f"  [{ticker}] EXIT REQUESTED: limits placed "
                  f"(long_ticket={'Y' if ts['long_exit_ticket'] else 'mkt-fallback'} "
                  f"short_ticket={'Y' if ts['short_exit_ticket'] else 'mkt-fallback/none'}) "
                  f"— finalize at hedge tick")

    # ── Delta hedge: combined calendar delta (before close) ───────────────────

    def _delta_hedge(self, ticker):
        ts = self._ts[ticker]

        # ── EXITING state: cancel any unfilled exit limit orders, market-flatten,
        #    then finalize PnL + trade-log row. By the time this runs (3:45 PM)
        #    limit orders have had ~1h45m to fill at mid. Anything still open
        #    must be force-flattened so we're not naked over weekend / holiday.
        if ts["state"] == "EXITING":
            for tkt_key, sym_key in [
                ("long_exit_ticket",  "put_symbol"),
                ("short_exit_ticket", "short_put_symbol"),
            ]:
                tkt = ts.get(tkt_key)
                if tkt is not None and tkt.Status not in (
                        OrderStatus.Filled, OrderStatus.Canceled, OrderStatus.Invalid):
                    tkt.Cancel()
                    sym = ts.get(sym_key)
                    qty = (self.Portfolio[sym].Quantity
                           if sym and self.Portfolio.ContainsKey(sym) else 0)
                    if qty != 0:
                        self._log(f"  [{ticker}] EXIT FALLBACK: limit unfilled, "
                                  f"market-flattening {qty} of {sym}")
                        self._filling_ticker = ticker
                        self.MarketOrder(sym, -qty)
                        self._filling_ticker = None
            # All 3 legs are now guaranteed flat (or recorded). Finalize.
            self._finalize_exit(ticker)
            return

        # ── Cancel unfilled long put limit order if still pending ─────────
        if ts["state"] == "ACTIVE" and ts.get("put_entry_fill", 0) == 0:
            ticket = ts.get("put_order_ticket")
            if ticket is not None:
                ticket.Cancel()
                self._log(f"  [{ticker}] Limit order for long put did not fill — cancelling")

                # Safety net: if any positions snuck through (QC timing edge case),
                # close them before resetting so they don't become phantoms.
                for sym_key in ("put_symbol", "short_put_symbol", "stock_symbol"):
                    sym = ts.get(sym_key)
                    if sym is None:
                        continue
                    try:
                        qty = self.Portfolio[sym].Quantity \
                              if self.Portfolio.ContainsKey(sym) else 0
                    except Exception:
                        qty = 0
                    if qty != 0:
                        self._log(f"  [{ticker}] CANCEL SAFETY: closing {qty} of {sym}")
                        self._filling_ticker = ticker
                        self.MarketOrder(sym, -qty)
                        self._filling_ticker = None

                self._reset(ticker)
                return

        # ── Cancel unfilled short put limit order if long is filled but short isn't ─
        # Long is in (paid debit) but short never filled at mid → unwind the long via
        # market order so we don't carry a naked long over close. No trade-log row
        # is written because the entry was incomplete.
        if (ts["state"] == "ACTIVE"
                and ts.get("put_entry_fill", 0) > 0
                and ts.get("short_put_entry_fill", 0) == 0
                and not ts.get("entry_aborted")):
            st_ticket = ts.get("short_put_order_ticket")
            if st_ticket is not None:
                st_ticket.Cancel()
                self._log(f"  [{ticker}] Short put limit did not fill — "
                          f"unwinding long put + resetting (no trade-log row)")
                ts["entry_aborted"] = True
                # Sell back the long put we already own (entry-unwind branch in
                # OnOrderEvent will call _reset when this fill comes back).
                self._filling_ticker = ticker
                self.MarketOrder(ts["put_symbol"], -ts["put_contracts"])
                self._filling_ticker = None
                # Safety net: nothing else should be open (stock hedge was deferred
                # to short-fill branch), but flatten anything that snuck through.
                for sym_key in ("short_put_symbol", "stock_symbol"):
                    sym = ts.get(sym_key)
                    if sym is None:
                        continue
                    try:
                        qty = self.Portfolio[sym].Quantity \
                              if self.Portfolio.ContainsKey(sym) else 0
                    except Exception:
                        qty = 0
                    if qty != 0:
                        self._log(f"  [{ticker}] CANCEL SAFETY: closing {qty} of {sym}")
                        self._filling_ticker = ticker
                        self.MarketOrder(sym, -qty)
                        self._filling_ticker = None
                return

        if ts["state"] != "ACTIVE" or ts["chain"] is None:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        entry_px = ts["stock_entry_price"]
        if entry_px > 0:
            _dev = (s_price - entry_px) / entry_px * 100
            ts["stock_max_up_pct"] = max(ts["stock_max_up_pct"], _dev)
            ts["stock_max_dn_pct"] = min(ts["stock_max_dn_pct"], _dev)

        # ── Step 1: Compute live Greeks for both legs ────────────────────────
        long_delta = long_gamma = long_theta = None
        short_delta = short_gamma = short_theta = None
        cur_iv = 0.0
        cur_short_iv = 0.0

        if (COMPUTE_OWN_GREEKS
                and ts.get("long_iv_smoother")  is not None
                and ts.get("short_iv_smoother") is not None):
            # ── Custom Black-Scholes greeks fed by per-leg smoothed IV ──
            # Each leg's smoother only ever sees its OWN IV samples — the
            # long and short IV histories are kept fully separate and
            # never mingled. See cal_greeks.IVSmoother.
            long_smooth  = ts["long_iv_smoother"].current_smooth()
            short_smooth = ts["short_iv_smoother"].current_smooth()
            if long_smooth is None or short_smooth is None:
                # Buffers should have at least the entry seed; this is a
                # safety net. Nothing usable to compute greeks from.
                return

            r = self._get_risk_free_rate()
            q = self._get_dividend_yield(ts["stock_symbol"])
            T_long  = (ts["put_symbol"].ID.Date       - self.Time).total_seconds() / (365.25 * 86400)
            T_short = (ts["short_put_symbol"].ID.Date - self.Time).total_seconds() / (365.25 * 86400)
            g_long  = bs_put_greeks(s_price, ts["put_symbol"].ID.StrikePrice,
                                    T_long,  r, q, long_smooth)
            g_short = bs_put_greeks(s_price, ts["short_put_symbol"].ID.StrikePrice,
                                    T_short, r, q, short_smooth)
            long_delta,  long_gamma,  long_theta  = g_long["delta"],  g_long["gamma"],  g_long["theta"]
            short_delta, short_gamma, short_theta = g_short["delta"], g_short["gamma"], g_short["theta"]
            cur_iv       = long_smooth
            cur_short_iv = short_smooth

            if GREEKS_VERBOSE:
                # Side-by-side custom-vs-QC sanity log (validation phase).
                qc_ld = qc_sd = qc_lg = qc_sg = None
                try:
                    for c in ts["chain"]:
                        if c.Symbol == ts["put_symbol"]:
                            qc_ld, qc_lg = c.Greeks.Delta, c.Greeks.Gamma
                        elif c.Symbol == ts.get("short_put_symbol"):
                            qc_sd, qc_sg = c.Greeks.Delta, c.Greeks.Gamma
                except Exception:
                    pass
                self._log(
                    f"  [{ticker}] GREEKS custom L_d={long_delta:.4f} S_d={short_delta:.4f} "
                    f"L_g={long_gamma:.4f} S_g={short_gamma:.4f} "
                    f"L_iv={long_smooth:.3f} S_iv={short_smooth:.3f} "
                    f"| QC L_d={qc_ld} S_d={qc_sd} L_g={qc_lg} S_g={qc_sg} "
                    f"| smooth_n L={ts['long_iv_smoother'].sample_count()} "
                    f"S={ts['short_iv_smoother'].sample_count()}"
                )
                self._log(
                    f"  [{ticker}] IV-SMOOTH long: {ts['long_iv_smoother'].reject_summary()}"
                )
                self._log(
                    f"  [{ticker}] IV-SMOOTH short: {ts['short_iv_smoother'].reject_summary()}"
                )
        else:
            # ── QC-greeks path (original behaviour) ──
            for c in ts["chain"]:
                if c.Symbol == ts["put_symbol"]:
                    long_delta = c.Greeks.Delta
                    long_gamma = c.Greeks.Gamma
                    long_theta = c.Greeks.Theta
                    try:
                        cur_iv = c.ImpliedVolatility
                    except Exception:
                        cur_iv = 0.0
                elif c.Symbol == ts.get("short_put_symbol"):
                    short_delta = c.Greeks.Delta
                    short_gamma = c.Greeks.Gamma
                    short_theta = c.Greeks.Theta
                    try:
                        cur_short_iv = c.ImpliedVolatility
                    except Exception:
                        cur_short_iv = 0.0

        if long_delta is None or long_delta == 0.0:
            return   # long put not in chain or stale — cannot compute delta

        if short_delta is None or short_delta == 0.0:
            self._log(f"  [{ticker}] HEDGE SKIPPED: short delta stale "
                      f"(long_d={long_delta:.4f}, stock_qty={ts['stock_qty']:.0f})")
            return

        # Combined option Greeks (long n puts, short n puts):
        n = ts["put_contracts"]

        # ── Theta watcher: exit if combined theta turns negative ──────────
        if THETA_WATCHER:
            _lt = long_theta if long_theta else 0.0
            _st = short_theta if short_theta else 0.0
            net_theta = (_lt - _st) * n * 100
            if net_theta < 0:
                self._log(f"  [{ticker}] THETA EXIT: net_theta={net_theta:.2f} < 0 — closing position")
                self._theta_exits += 1
                self._exit_position(ticker)
                return

        if not DELTA_HEDGE:
            return

        option_delta = (long_delta - short_delta) * n * 100

        stock_delta    = ts["stock_qty"]
        position_delta = stock_delta + option_delta

        # ── Step 2: Hedge trigger ────────────────────────────────────────────
        option_exposure = abs(n * 100)

        if HEDGE_MODE in ("gamma", "theta"):
            _lg = long_gamma if long_gamma else 0.0
            _sg = short_gamma if short_gamma else 0.0
            _lt = long_theta if long_theta else 0.0
            _st = short_theta if short_theta else 0.0

            net_gamma = (_lg - _sg) * n * 100
            net_theta = (_lt - _st) * n * 100
            total_gamma = abs(net_gamma)

            if HEDGE_MODE == "theta":
                daily_theta_pos = abs(net_theta) / 365.0
                pnl_tol = max(THETA_K * daily_theta_pos, MIN_TOLERANCE)
            else:
                pnl_tol = PNL_TOLERANCE

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
                    return

        else:
            if RV_SIGMA:
                rv = self._calc_realized_vol(ts["stock_symbol"], 30)
                if rv <= 0:
                    return
                daily_sigma_frac = rv / (252 ** 0.5)
            else:
                # Short-leg IV is the closest forward-looking projection of the
                # underlying's near-term realized vol — its time-to-expiry is
                # the smallest, so it best estimates "what's expected to happen
                # tomorrow," which is exactly what the daily delta-hedge band
                # needs to anticipate. Fall back to long-leg live IV → short
                # entry IV → long entry IV in that order if quotes are stale.
                sigma_iv = (cur_short_iv if cur_short_iv > 0
                            else cur_iv if cur_iv > 0
                            else ts.get("short_put_entry_iv", 0.0) or
                                 ts.get("put_entry_iv", 0.0))
                if sigma_iv <= 0:
                    return
                daily_sigma_frac = sigma_iv / (252 ** 0.5)

            tolerance = D_mult * daily_sigma_frac * option_exposure
            if abs(position_delta) <= tolerance:
                return

        # ── Step 3: Execute hedge ────────────────────────────────────────────
        target = round(-option_delta)
        adj    = target - ts["stock_qty"]
        if adj == 0:
            return

        # self._log(f"  [{ticker}] HEDGE: long_d={long_delta:.4f} short_d={short_delta:.4f} "
        #          f"opt_delta={option_delta:+.0f} stock_qty={ts['stock_qty']} "
        #          f"target={target} adj={adj:+.0f} price={s_price:.2f}")

        self._filling_ticker = ticker
        self.MarketOrder(ts["stock_symbol"], adj)
        self._filling_ticker = None

        ts["hedge_count"] += 1
        ts["last_hedge_price"] = s_price

    # ── Helpers (migrated to cal_helpers.py / cal_greeks.py) ──────────────────
    # _log, _ol, OnEndOfAlgorithm and other utility helpers are bound at the
    # top of this class via class-attribute assignment from cal_helpers /
    # cal_greeks. See those modules for implementations.
