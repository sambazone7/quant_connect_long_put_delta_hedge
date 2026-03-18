# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import math
from cal_config import *
# endregion


class EarningsCalendarPutMultiTicker(QCAlgorithm):
    """
    
    """

    # ── Initialise ────────────────────────────────────────────────────────────

    def Initialize(self):
        self.SetStartDate(2022, 1, 1)
        self.SetEndDate(2026, 2, 20)
        self.SetCash(20_000_000)

        tickers = list(MANUAL_EARNINGS_DATES.keys())

        # Per-ticker state dict
        self._ts = {}
        self._max_concurrent = 0   # peak number of tickers held simultaneously
        self._all_lines = []       # collect ALL log lines for ObjectStore
        self._filling_ticker = None  # set around our own orders to distinguish from auto-liquidation
        self._total_fees = 0.0     # accumulated fees across all tickers and orders

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
                "put_contracts":        0,        # same qty for both legs
                "put_entry_fill":       0.0,      # long put entry fill
                "put_exit_fill":        0.0,      # long put exit fill
                "short_put_entry_fill": 0.0,      # short put entry fill
                "short_put_exit_fill":  0.0,      # short put exit fill
                "short_put_expiry":     None,     # date — drives exit timing
                "put_entry_iv":         0.0,      # IV of long put at entry
                "short_put_entry_iv":   0.0,      # IV of short put at entry
                "put_order_ticket":     None,     # limit order ticket for long put
                "pending_short_put":    False,    # True until short put placed after long put fills
                "pending_hedge_shares": 0,
                "stock_qty":            0,
                "stock_cost_basis":     0.0,
                "stock_realized":       0.0,
                "last_hedge_price":     0.0,
                "entry_earnings":       None,
                "entry_rv":             0.0,
                # chain cache
                "chain": None,
                # data
                "earnings_dates":  dates,
                "traded_earnings": set(),
                "trade_log":       [],
                "force_exited":    False,
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
                    if ts["state"] in ("ACTIVE", "EMERGENCY_PENDING"):
                        if ts["stock_qty"] > 0 and split.SplitFactor > 0:
                            ts["stock_qty"] = round(ts["stock_qty"] / split.SplitFactor)
                    if ts["state"] == "ACTIVE" and not ts.get("force_exited"):
                        self._emergency_exit(ticker, f"{ticker} split {split.SplitFactor}")

    # ── Order fill tracking ──────────────────────────────────────────────────

    def OnOrderEvent(self, orderEvent):
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
            if ts["state"] not in ("ACTIVE", "EMERGENCY_PENDING"):
                continue

            # ── Long put fill ────────────────────────────────────────
            if symbol == ts.get("put_symbol"):
                if fill_qty > 0:
                    ts["put_entry_fill"] = fill_price
                    # Deferred: sell short put now that long put is filled
                    if ts.get("pending_short_put"):
                        self._filling_ticker = ticker
                        self.MarketOrder(ts["short_put_symbol"], -ts["put_contracts"])
                        self._filling_ticker = None
                        ts["pending_short_put"] = False
                    # Deferred: place stock hedge
                    pending = ts.get("pending_hedge_shares", 0)
                    if pending != 0:
                        self._filling_ticker = ticker
                        self.MarketOrder(ts["stock_symbol"], pending)
                        self._filling_ticker = None
                        ts["pending_hedge_shares"] = 0
                else:
                    ts["put_exit_fill"] = fill_price
                    # Detect auto-liquidation: long put sold but NOT by our code
                    if self._filling_ticker != ticker and not ts.get("force_exited"):
                        self._log(f"  [{self.Time.date()}] AUTO-LIQUIDATION detected: "
                                 f"long put {symbol} sold at ${fill_price:.2f} (likely stock split)")
                        self._immediate_close_all(ticker, "long put auto-liquidation")
                return

            # ── Short put fill ───────────────────────────────────────
            if symbol == ts.get("short_put_symbol"):
                if fill_qty < 0:
                    # We sold the short put (entry)
                    ts["short_put_entry_fill"] = fill_price
                else:
                    # We bought back the short put (exit) — likely ASSIGNMENT
                    ts["short_put_exit_fill"] = fill_price
                    # Detect assignment / auto-liquidation of short put
                    if self._filling_ticker != ticker and not ts.get("force_exited"):
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

        Tries all strikes from ATM downward (highest ≤ stock_price first).
        Returns ((long_put, short_put), "") or (None, reason_string).
        """
        puts = [c for c in chain if c.Right == OptionRight.Put]
        if not puts:
            return (None, "no puts in chain")

        # All candidate strikes, highest (closest to ATM) first
        valid_strikes = sorted(set(p.Strike for p in puts if p.Strike <= stock_price),
                               reverse=True)
        if not valid_strikes:
            return (None, f"no strikes <= ${stock_price:.2f}")

        # Track the best failure reason (from the ATM strike — most informative)
        first_reason = ""

        for strike in valid_strikes:
            strike_puts = [p for p in puts if p.Strike == strike]
            if len(strike_puts) < 2:
                if not first_reason:
                    first_reason = f"K={strike} only {len(strike_puts)} expiry"
                continue

            # Split into before / after earnings
            before = [p for p in strike_puts if p.Expiry.date() < earnings_date]
            after  = [p for p in strike_puts if p.Expiry.date() >= earnings_date]

            if not before or not after:
                if not first_reason:
                    expiries = sorted(set(p.Expiry.date() for p in strike_puts))
                    first_reason = (f"K={strike} before={len(before)} after={len(after)} "
                                    f"expiries={expiries} earn={earnings_date}")
                continue

            # Closest expiry to earnings in each group
            short_put = max(before, key=lambda p: p.Expiry)   # closest before
            long_put  = min(after,  key=lambda p: p.Expiry)   # closest after

            spread_days = (long_put.Expiry.date() - short_put.Expiry.date()).days
            if spread_days > MAX_SPREAD_DAYS:
                if not first_reason:
                    first_reason = (f"K={strike} spread={spread_days}d > {MAX_SPREAD_DAYS}d "
                                    f"(short_exp={short_put.Expiry.date()}, "
                                    f"long_exp={long_put.Expiry.date()})")
                continue

            return ((long_put, short_put), "")

        # None of the strikes had a valid pair
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
        ts["entry_earnings"]      = earnings_dt
        ts["entry_rv"]            = rv
        ts["long_spread_entry"]   = round(abs(long_put.BidPrice - long_put.AskPrice) * 100 * n_contracts)
        ts["short_spread_entry"]  = round(abs(short_put.BidPrice - short_put.AskPrice) * 100 * n_contracts)

        # ── Pin both contracts so the universe filter can never unsubscribe them ──
        # Also set NullAssignmentModel on each contract individually — the
        # parent chain's model does NOT propagate to manually-added contracts.
        _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
        long_sec  = self.AddOptionContract(long_put.Symbol, _res)
        short_sec = self.AddOptionContract(short_put.Symbol, _res)
        long_sec.SetOptionAssignmentModel(NullAssignmentModel())
        short_sec.SetOptionAssignmentModel(NullAssignmentModel())

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

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _exit_position(self, ticker):
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE":
            return
        if ts.get("force_exited"):
            return   # already emergency-closed (e.g. stock split)

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

        # Save quantities before orders (OnOrderEvent will update them)
        n_contracts = ts["put_contracts"]
        n_shares    = ts["stock_qty"]

        # Record exit bid-ask spread costs before closing
        try:
            _long_sec  = self.Securities[ts["put_symbol"]]
            ts["long_spread_exit"] = round(abs(_long_sec.BidPrice - _long_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            ts["long_spread_exit"] = 0
        try:
            _short_sec = self.Securities[ts["short_put_symbol"]]
            ts["short_spread_exit"] = round(abs(_short_sec.BidPrice - _short_sec.AskPrice) * 100 * n_contracts)
        except Exception:
            ts["short_spread_exit"] = 0

        # Close all three legs — OnOrderEvent captures actual fill prices
        self._filling_ticker = ticker
        if n_contracts > 0:
            self.MarketOrder(ts["put_symbol"], -n_contracts)          # sell long put
            self.MarketOrder(ts["short_put_symbol"], n_contracts)     # buy back short put
        if n_shares != 0:
            self.MarketOrder(ts["stock_symbol"], -n_shares)
        self._filling_ticker = None

        # PnL from actual fill prices (set by OnOrderEvent)
        long_pnl  = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
        short_pnl = (ts["short_put_entry_fill"] - ts["short_put_exit_fill"]) * n_contracts * 100
        stk_pnl   = ts["stock_realized"]
        total_pnl = long_pnl + short_pnl + stk_pnl

        # Stock % change
        entry_px = ts["stock_entry_price"]
        exit_px  = self.Securities[ts["stock_symbol"]].Price
        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        ed = ts["entry_earnings"].date()

        _short_iv = ts["short_put_entry_iv"]
        _long_iv  = ts["put_entry_iv"]
        _rv       = ts["entry_rv"]
        _iv_spread = _long_iv - _short_iv
        _short_iv_rv = _short_iv / _rv if _rv > 0 else 0.0

        self._log(f"  [{ticker}] EXIT: LongPnL=${long_pnl:+,.2f} "
                 f"ShortPnL=${short_pnl:+,.2f} StkPnL=${stk_pnl:+,.2f} "
                 f"Total=${total_pnl:+,.2f} "
                 f"IVspread={_iv_spread:.1%} ShortIV/RV={_short_iv_rv:.2f}")

        ts["trade_log"].append({
            "earnings":           ed,
            "n_contracts":        n_contracts,
            "long_pnl":           long_pnl,
            "short_pnl":          short_pnl,
            "stk_pnl":            stk_pnl,
            "stk_chg_pct":        stk_chg_pct,
            "total":              total_pnl,
            "iv_entry":           _long_iv,
            "iv_exit":            put_exit_iv,
            "rv":                 _rv,
            "iv_spread_entry":    _long_iv - _short_iv,
            "short_iv_entry":     _short_iv,
            "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
            "long_spread_entry":  ts["long_spread_entry"],
            "short_spread_entry": ts["short_spread_entry"],
            "long_spread_exit":   ts["long_spread_exit"],
            "short_spread_exit":  ts["short_spread_exit"],
        })

        # ── Post-exit reconciliation: verify portfolio is actually flat ────
        for lbl, sym in [("long_put", ts["put_symbol"]),
                         ("short_put", ts["short_put_symbol"]),
                         ("stock", ts["stock_symbol"])]:
            if sym is None:
                continue
            try:
                rem = self.Portfolio[sym].Quantity \
                      if self.Portfolio.ContainsKey(sym) else 0
            except Exception:
                rem = 0
            if rem != 0:
                self._log(f"  [{ticker}] POST-EXIT WARNING: {lbl} still has {rem} "
                         f"units after exit — positions not flat!")

        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    # ── Delta hedge: combined calendar delta (before close) ───────────────────

    def _delta_hedge(self, ticker):
        ts = self._ts[ticker]

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

        # ── Warm-up: if exit is tomorrow, re-subscribe both puts now ─────
        if ts["state"] == "ACTIVE" and ts.get("short_put_expiry"):
            today = self.Time.date()
            exit_day = self._offset_trading_days(ticker, ts["short_put_expiry"], -EXIT_DAYS_BEFORE)
            if exit_day is not None:
                tomorrow = self._offset_trading_days(ticker, today, 1)
                if tomorrow == exit_day:
                    _res = Resolution.Hour if HOURLY_BARS else Resolution.Minute
                    if ts.get("put_symbol"):
                        self.AddOptionContract(ts["put_symbol"], _res)
                    if ts.get("short_put_symbol"):
                        self.AddOptionContract(ts["short_put_symbol"], _res)

        if ts["state"] != "ACTIVE" or ts["chain"] is None:
            return

        if not DELTA_HEDGE:
            return

        stock   = self.Securities[ts["stock_symbol"]]
        s_price = stock.Price
        if s_price <= 0:
            return

        # ── Step 1: Compute live position delta ──────────────────────────────
        long_delta  = None
        short_delta = None
        cur_iv      = 0.0

        for c in ts["chain"]:
            if c.Symbol == ts["put_symbol"]:
                long_delta = c.Greeks.Delta
                try:
                    cur_iv = c.ImpliedVolatility
                except Exception:
                    cur_iv = 0.0
            elif c.Symbol == ts.get("short_put_symbol"):
                short_delta = c.Greeks.Delta

        if long_delta is None or long_delta == 0.0:
            return   # long put not in chain or stale — cannot compute delta

        # If short put not in chain or returning stale 0.0 Greeks
        # (common near expiry), skip hedge to preserve current stock position.
        if short_delta is None or short_delta == 0.0:
            self._log(f"  [{ticker}] HEDGE SKIPPED: short delta stale "
                      f"(long_d={long_delta:.4f}, stock_qty={ts['stock_qty']:.0f})")
            return

        # Combined option delta:
        #   long put:  +n_contracts → long_delta × n × 100
        #   short put: −n_contracts → −short_delta × n × 100
        n = ts["put_contracts"]
        option_delta = (long_delta - short_delta) * n * 100

        stock_delta    = ts["stock_qty"]
        position_delta = stock_delta + option_delta

        # ── Step 2: Daily sigma fraction ─────────────────────────────────────
        if RV_SIGMA:
            rv = self._calc_realized_vol(ts["stock_symbol"], 30)
            if rv <= 0:
                return
            daily_sigma_frac = rv / (252 ** 0.5)
        else:
            entry_iv = ts["put_entry_iv"]
            if entry_iv <= 0:
                return
            daily_sigma_frac = entry_iv / (252 ** 0.5)

        # ── Step 3: Tolerance (in shares) ────────────────────────────────────
        option_exposure = abs(n * 100)
        tolerance = D_mult * daily_sigma_frac * option_exposure

        # ── Step 4: Hedge decision ───────────────────────────────────────────
        if abs(position_delta) <= tolerance:
            return   # within tolerance — do nothing

        # Re-hedge to delta-neutral: target stock qty offsets option delta
        target = round(-option_delta)
        adj    = target - ts["stock_qty"]
        if adj == 0:
            return

        self._log(f"  [{ticker}] HEDGE: long_d={long_delta:.4f} short_d={short_delta:.4f} "
                 f"opt_delta={option_delta:+.0f} stock_qty={ts['stock_qty']} "
                 f"target={target} adj={adj:+.0f} price={s_price:.2f}")

        self._filling_ticker = ticker
        self.MarketOrder(ts["stock_symbol"], adj)
        self._filling_ticker = None

        ts["last_hedge_price"] = s_price

    # ── Helpers ───────────────────────────────────────────────────────────────

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
        """Compute annualized realized volatility from daily log returns."""
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

    # ── Immediate close (assignment / auto-liquidation detected in OnOrderEvent) ─

    def _immediate_close_all(self, ticker, reason):
        """Close ALL remaining legs immediately from inside OnOrderEvent.
        Used when an unexpected fill (assignment / auto-liquidation) is detected
        so that no positions are left orphaned."""
        ts = self._ts[ticker]
        if ts.get("force_exited"):
            return
        ts["force_exited"] = True

        self._log(f"  [{self.Time.date()}] IMMEDIATE CLOSE ({reason}): "
                 f"ticker={ticker} — closing all legs now")

        self._filling_ticker = ticker

        # ── Close long put if still held ──────────────────────────────
        if ts.get("put_symbol"):
            try:
                long_qty = self.Portfolio[ts["put_symbol"]].Quantity \
                           if self.Portfolio.ContainsKey(ts["put_symbol"]) else 0
            except Exception:
                long_qty = 0
            if long_qty != 0:
                self.MarketOrder(ts["put_symbol"], -long_qty)

        # ── Close short put if still held ─────────────────────────────
        if ts.get("short_put_symbol"):
            try:
                short_qty = self.Portfolio[ts["short_put_symbol"]].Quantity \
                            if self.Portfolio.ContainsKey(ts["short_put_symbol"]) else 0
            except Exception:
                short_qty = 0
            if short_qty != 0:
                self.MarketOrder(ts["short_put_symbol"], -short_qty)

        # ── Close stock ───────────────────────────────────────────────
        try:
            stk_qty = self.Portfolio[ts["stock_symbol"]].Quantity \
                      if self.Portfolio.ContainsKey(ts["stock_symbol"]) else 0
        except Exception:
            stk_qty = 0
        if stk_qty != 0:
            self.MarketOrder(ts["stock_symbol"], -stk_qty)

        self._filling_ticker = None

        # ── P&L from actual fills (set by OnOrderEvent during the orders above) ─
        n_contracts = ts["put_contracts"]
        long_pnl  = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
        short_pnl = (ts["short_put_entry_fill"] - ts["short_put_exit_fill"]) * n_contracts * 100
        stk_pnl   = ts["stock_realized"]
        total     = long_pnl + short_pnl + stk_pnl

        self._log(f"  [{self.Time.date()}]  LongPnL=${long_pnl:+,.2f} "
                 f"ShortPnL=${short_pnl:+,.2f} StkPnL=${stk_pnl:+,.2f} "
                 f"Total=${total:+,.2f}")

        # Stock % change
        entry_px = ts["stock_entry_price"]
        exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()

        # Store the short put strike so FLAT-state orphan cleanup can compute
        # assignment stock P&L if delivery arrives after this reset.
        short_strike = 0.0
        if ts.get("short_put_symbol"):
            try:
                short_strike = float(self.Securities[ts["short_put_symbol"]].Symbol.ID.StrikePrice)
            except Exception:
                short_strike = 0.0

        _short_iv = ts["short_put_entry_iv"]
        _long_iv  = ts["put_entry_iv"]
        _rv       = ts["entry_rv"]

        ts["trade_log"].append({
            "earnings":           ed,
            "n_contracts":        n_contracts,
            "long_pnl":           long_pnl,
            "short_pnl":          short_pnl,
            "stk_pnl":            stk_pnl,
            "stk_chg_pct":        stk_chg_pct,
            "total":              total,
            "iv_entry":           _long_iv,
            "iv_exit":            0.0,
            "rv":                 _rv,
            "iv_spread_entry":    _long_iv - _short_iv,
            "short_iv_entry":     _short_iv,
            "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
            "_assign_strike":     short_strike,   # used by FLAT orphan cleanup
        })
        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    # ── Emergency exit (stock split from OnData — deferred) ──────────────────

    def _emergency_exit(self, ticker, reason):
        """Mark one ticker's position as EMERGENCY_PENDING.
        Does NOT place orders or calculate P&L — deferred to _finalize_emergency_exit.
        Used only from OnData (stock split detection), NOT from OnOrderEvent."""
        ts = self._ts[ticker]
        if ts["state"] != "ACTIVE" or ts.get("force_exited"):
            return
        ts["force_exited"] = True
        ts["state"]        = "EMERGENCY_PENDING"
        self._log(f"  [{self.Time.date()}] EMERGENCY EXIT ({reason}): "
                 f"ticker={ticker} (pending close)")

    def _finalize_emergency_exit(self, ticker):
        """Called from _manage_position when state is EMERGENCY_PENDING.
        Closes remaining positions, calculates P&L, logs the trade, resets."""
        ts = self._ts[ticker]
        if ts["state"] != "EMERGENCY_PENDING":
            return

        self._log(f"  [{self.Time.date()}] FINALIZING EMERGENCY EXIT for {ticker}")

        # ── Close long put if still open ──────────────────────────────
        if ts.get("put_symbol"):
            long_qty = 0
            try:
                if self.Portfolio.ContainsKey(ts["put_symbol"]):
                    long_qty = self.Portfolio[ts["put_symbol"]].Quantity
            except Exception:
                long_qty = 0
            if long_qty != 0:
                self._filling_ticker = ticker
                self.MarketOrder(ts["put_symbol"], -long_qty)
                self._filling_ticker = None

        # ── Close remaining stock position ──────────────────────────
        actual_stock_qty = 0
        if self.Portfolio.ContainsKey(ts["stock_symbol"]):
            actual_stock_qty = self.Portfolio[ts["stock_symbol"]].Quantity

        if actual_stock_qty != 0:
            self._filling_ticker = ticker
            self.MarketOrder(ts["stock_symbol"], -actual_stock_qty)
            self._filling_ticker = None

        # ── Buy back short put if still open ─────────────────────────
        if ts.get("short_put_symbol"):
            short_qty = 0
            if self.Portfolio.ContainsKey(ts["short_put_symbol"]):
                short_qty = self.Portfolio[ts["short_put_symbol"]].Quantity
            if short_qty != 0:
                self._filling_ticker = ticker
                self.MarketOrder(ts["short_put_symbol"], -short_qty)
                self._filling_ticker = None

        # ── P&L calculation ─────────────────────────────────────────
        n_contracts = ts["put_contracts"]
        long_pnl  = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
        short_pnl = (ts["short_put_entry_fill"] - ts["short_put_exit_fill"]) * n_contracts * 100
        stk_pnl   = ts["stock_realized"]
        total     = long_pnl + short_pnl + stk_pnl

        self._log(f"  [{self.Time.date()}]  LongPnL=${long_pnl:+,.2f} "
                 f"ShortPnL=${short_pnl:+,.2f} StkPnL=${stk_pnl:+,.2f} "
                 f"Total=${total:+,.2f}")

        # Stock % change
        entry_px = ts["stock_entry_price"]
        exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
        stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

        ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()

        _short_iv = ts["short_put_entry_iv"]
        _long_iv  = ts["put_entry_iv"]
        _rv       = ts["entry_rv"]

        ts["trade_log"].append({
            "earnings":           ed,
            "n_contracts":        n_contracts,
            "long_pnl":           long_pnl,
            "short_pnl":          short_pnl,
            "stk_pnl":            stk_pnl,
            "stk_chg_pct":        stk_chg_pct,
            "total":              total,
            "iv_entry":           _long_iv,
            "iv_exit":            0.0,
            "rv":                 _rv,
            "iv_spread_entry":    _long_iv - _short_iv,
            "short_iv_entry":     _short_iv,
            "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
            "long_spread_entry":  ts["long_spread_entry"],
            "short_spread_entry": ts["short_spread_entry"],
            "long_spread_exit":   0,
            "short_spread_exit":  0,
        })
        ts["traded_earnings"].add(ed)
        self._reset(ticker)

    # ── Hourly orphan check ──────────────────────────────────────────────────

    def _check_orphaned_positions(self, ticker):
        """Hourly safety net:
        1. ACTIVE state — if we hold long put but NOT short put → short was assigned.
        2. FLAT state — if stock is lingering from a previous assignment delivery
           that arrived after _immediate_close_all already reset state → sell it."""
        ts = self._ts[ticker]

        # ── FLAT-state cleanup: sell orphaned assignment stock ──────────
        if ts["state"] == "FLAT":
            # Once we've already cleaned up orphan stock for this cycle, don't
            # repeat — corporate actions (spinoffs, etc.) can keep delivering
            # new shares, and each repeat would double the P&L correction.
            if ts.get("orphan_cleaned"):
                return

            stk_qty = 0
            try:
                if self.Portfolio.ContainsKey(ts["stock_symbol"]):
                    stk_qty = self.Portfolio[ts["stock_symbol"]].Quantity
            except Exception:
                stk_qty = 0
            if stk_qty != 0:
                sell_price = self.Securities[ts["stock_symbol"]].Price
                self._log(f"  [{self.Time.date()}] ORPHAN STOCK (FLAT) for {ticker}: "
                         f"{stk_qty} shares at ${sell_price:.2f} — selling immediately")
                self._filling_ticker = ticker
                self.MarketOrder(ts["stock_symbol"], -stk_qty)
                self._filling_ticker = None

                # Mark as cleaned so we don't repeat this for the same cycle
                ts["orphan_cleaned"] = True

                # ── Record the assignment stock P&L as a correction to
                #    the last trade_log entry (written by _immediate_close_all
                #    before the assignment stock arrived) ──────────────────
                if ts["trade_log"]:
                    last = ts["trade_log"][-1]
                    # Assignment stock was bought at strike; we're selling at market
                    # stk_qty is positive for long stock (put assignment delivers long)
                    # The fill price in OnOrderEvent was ignored (FLAT state),
                    # so estimate: bought at strike, sold at current market.
                    assign_strike = last.get("_assign_strike", 0.0)
                    if assign_strike > 0:
                        assign_loss = (sell_price - assign_strike) * abs(stk_qty)
                    else:
                        assign_loss = 0.0
                    last["stk_pnl"]  += assign_loss
                    last["total"]    += assign_loss
                    self._log(f"  [{self.Time.date()}] ORPHAN P&L correction: "
                             f"assign_strike=${assign_strike:.2f} "
                             f"sell_price=${sell_price:.2f} "
                             f"shares={abs(stk_qty)} "
                             f"loss=${assign_loss:+,.2f} "
                             f"new_total=${last['total']:+,.2f}")
            return

        if ts["state"] != "ACTIVE":
            return

        put_sym   = ts.get("put_symbol")
        short_sym = ts.get("short_put_symbol")
        if not put_sym or not short_sym:
            return

        # Read actual portfolio quantities
        long_qty  = 0
        short_qty = 0
        stk_qty   = 0
        try:
            if self.Portfolio.ContainsKey(put_sym):
                long_qty = self.Portfolio[put_sym].Quantity
        except Exception:
            long_qty = 0
        try:
            if self.Portfolio.ContainsKey(short_sym):
                short_qty = self.Portfolio[short_sym].Quantity
        except Exception:
            short_qty = 0
        try:
            if self.Portfolio.ContainsKey(ts["stock_symbol"]):
                stk_qty = self.Portfolio[ts["stock_symbol"]].Quantity
        except Exception:
            stk_qty = 0

        # Orphan condition: we hold long put but short put is gone (assigned away)
        if long_qty > 0 and short_qty == 0:
            self._log(f"  [{self.Time.date()}] ORPHAN DETECTED for {ticker}: "
                     f"long_qty={long_qty}, short_qty={short_qty}, stk_qty={stk_qty} "
                     f"— closing all positions")

            ts["force_exited"] = True
            self._filling_ticker = ticker

            # Close long put
            self.MarketOrder(put_sym, -long_qty)

            # Close stock (could be hedge stock + assignment-delivered stock)
            if stk_qty != 0:
                self.MarketOrder(ts["stock_symbol"], -stk_qty)

            self._filling_ticker = None

            # P&L from actual fills
            n_contracts = ts["put_contracts"]
            long_pnl  = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
            short_pnl = (ts["short_put_entry_fill"] - ts["short_put_exit_fill"]) * n_contracts * 100
            stk_pnl   = ts["stock_realized"]
            total     = long_pnl + short_pnl + stk_pnl

            self._log(f"  [{self.Time.date()}] ORPHAN CLEANUP: "
                     f"LongPnL=${long_pnl:+,.2f} ShortPnL=${short_pnl:+,.2f} "
                     f"StkPnL=${stk_pnl:+,.2f} Total=${total:+,.2f}")

            # Stock % change
            entry_px = ts["stock_entry_price"]
            exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
            stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

            ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()

            _short_iv = ts["short_put_entry_iv"]
            _long_iv  = ts["put_entry_iv"]
            _rv       = ts["entry_rv"]

            ts["trade_log"].append({
                "earnings":           ed,
                "n_contracts":        n_contracts,
                "long_pnl":           long_pnl,
                "short_pnl":          short_pnl,
                "stk_pnl":            stk_pnl,
                "stk_chg_pct":        stk_chg_pct,
                "total":              total,
                "iv_entry":           _long_iv,
                "iv_exit":            0.0,
                "rv":                 _rv,
                "iv_spread_entry":    _long_iv - _short_iv,
                "short_iv_entry":     _short_iv,
                "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
                "long_spread_entry":  ts["long_spread_entry"],
                "short_spread_entry": ts["short_spread_entry"],
                "long_spread_exit":   0,
                "short_spread_exit":  0,
            })
            ts["traded_earnings"].add(ed)
            self._reset(ticker)

    def _reset(self, ticker):
        ts = self._ts[ticker]
        ts["state"]               = "FLAT"
        ts["put_symbol"]          = None
        ts["short_put_symbol"]    = None
        ts["put_contracts"]       = 0
        ts["put_entry_fill"]      = 0.0
        ts["put_exit_fill"]       = 0.0
        ts["short_put_entry_fill"]= 0.0
        ts["short_put_exit_fill"] = 0.0
        ts["short_put_expiry"]    = None
        ts["put_entry_iv"]        = 0.0
        ts["short_put_entry_iv"]  = 0.0
        ts["put_order_ticket"]    = None
        ts["pending_short_put"]   = False
        ts["pending_hedge_shares"]= 0
        ts["stock_qty"]           = 0
        ts["stock_cost_basis"]    = 0.0
        ts["stock_realized"]      = 0.0
        ts["last_hedge_price"]    = 0.0
        ts["stock_entry_price"]   = 0.0
        ts["entry_earnings"]      = None
        ts["entry_rv"]            = 0.0
        ts["force_exited"]        = False
        # Note: orphan_cleaned is NOT reset here — it stays True until
        # _enter_position clears it, preventing repeated cleanup in FLAT state.
        ts["total_fees"]          = 0.0
        ts["long_spread_entry"]   = 0
        ts["short_spread_entry"]  = 0
        ts["long_spread_exit"]    = 0
        ts["short_spread_exit"]   = 0

    # ── End-of-backtest summary ───────────────────────────────────────────────

    def _log(self, msg):
        """Log a message to QC log AND collect for ObjectStore."""
        stamped = f"{self.Time} {msg}"
        self.Log(msg)
        self._all_lines.append(stamped)

    def _ol(self, lines, msg):
        """Log a message AND collect it for summary + ObjectStore."""
        self._log(msg)
        lines.append(msg)

    def OnEndOfAlgorithm(self):
        lines = []          # collected for ObjectStore (bypasses 100 KB log cap)
        grand_total = 0.0
        grand_trades = 0
        grand_wins   = 0
        grand_attempts   = 0
        grand_no_pair    = 0
        grand_low_debit  = 0
        grand_other_skip = 0

        for ticker in self._ts:
            ts = self._ts[ticker]
            trade_log = ts["trade_log"]
            n         = len(trade_log)

            # ── Skip counters for this ticker ──
            _att  = ts["entry_attempts"]
            _np   = ts["skips_no_pair"]
            _ld   = ts["skips_low_debit"]
            _ot   = ts["skips_other"]
            _skipped_total = _np + _ld + _ot
            grand_attempts   += _att
            grand_no_pair    += _np
            grand_low_debit  += _ld
            grand_other_skip += _ot

            self._ol(lines, f"{'='*80}")
            if n == 0:
                self._ol(lines, f"  {ticker} SUMMARY  |  No trades completed")
                if _att > 0:
                    self._ol(lines, f"  Entries attempted: {_att}  |  Skipped: {_skipped_total} (no_pair={_np}, low_debit={_ld}, other={_ot})")
                self._ol(lines, f"{'='*80}")
                continue

            totals = {"long": 0.0, "short": 0.0, "stk": 0.0, "total": 0.0}
            valid  = [t for t in trade_log if t["iv_exit"] != 0.0]
            wins   = sum(1 for t in valid if t["total"] >= 0)
            nv     = len(valid)
            grand_trades += nv
            grand_wins   += wins

            self._ol(lines, f"  {ticker} SUMMARY  |  {nv} trade(s)  |  Wins: {wins}/{nv}  (skipped {n - nv} w/ iv_exit=0)")
            self._ol(lines, f"  Entries attempted: {_att}  |  Skipped: {_skipped_total} (no_pair={_np}, low_debit={_ld}, other={_ot})")
            self._ol(lines, f"{'-'*80}")
            self._ol(lines,
                f"  {'Earnings':<12}"
                f" {'n':>5}"
                f" {'Long PnL':>12}"
                f" {'Short PnL':>12}"
                f" {'Stock PnL':>12}"
                f" {'Stk Chg%':>9}"
                f" {'Combined':>12}"
                f" {'IV entry':>9}"
                f" {'IV exit':>8}"
                f" {'IVspread':>9}"
                f" {'ShIV/RV':>8}"
                f" {'IV chg':>7}"
                f" {'IV/RV':>6}"
                f" {'LSpEn':>7}"
                f" {'SSpEn':>7}"
                f" {'LSpEx':>7}"
                f" {'SSpEx':>7}"
            )
            self._ol(lines, f"  {'-'*160}")

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
                iv_spr  = t.get("iv_spread_entry", 0.0)
                sh_rv   = t.get("short_iv_rv", 0.0)
                nc = t.get("n_contracts", 0)
                _lse = t.get("long_spread_entry", 0)
                _sse = t.get("short_spread_entry", 0)
                _lsx = t.get("long_spread_exit", 0)
                _ssx = t.get("short_spread_exit", 0)
                self._ol(lines,
                    f"  {tag} {t['earnings']!s:<11}"
                    f"  {nc:>5}"
                    f"  ${t['long_pnl']:>+10,.2f}"
                    f"  ${t['short_pnl']:>+10,.2f}"
                    f"  ${t['stk_pnl']:>+10,.2f}"
                    f"  {chg:>+7.1f}%"
                    f"  ${t['total']:>+10,.2f}"
                    f"  {t['iv_entry']:>8.1%}"
                    f"  {t['iv_exit']:>7.1%}"
                    f"  {iv_spr:>8.1%}"
                    f"  {sh_rv:>8.2f}"
                    f"  {iv_chg:>+6.0f}%"
                    f"  {ratio:>6}"
                    f"  {_lse:>7}"
                    f"  {_sse:>7}"
                    f"  {_lsx:>7}"
                    f"  {_ssx:>7}"
                )
                totals["long"]  += t["long_pnl"]
                totals["short"] += t["short_pnl"]
                totals["stk"]   += t["stk_pnl"]
                totals["total"] += t["total"]

            printed = n - skipped
            avg = totals["total"] / printed if printed > 0 else 0.0
            self._ol(lines, f"  {'-'*160}")
            self._ol(lines,
                f"  {'TOTAL':<15}"
                f"  ${totals['long']:>+10,.2f}"
                f"  ${totals['short']:>+10,.2f}"
                f"  ${totals['stk']:>+10,.2f}"
                f"  {'':>9}"
                f"  ${totals['total']:>+10,.2f}"
            )
            self._ol(lines, f"  Avg PnL/trade: ${avg:+,.2f}")
            self._ol(lines, f"{'='*80}")
            grand_total += totals["total"]

        if len(self._ts) > 1:
            grand_skipped = grand_no_pair + grand_low_debit + grand_other_skip
            self._ol(lines, f"{'='*80}")
            self._ol(lines, f"  ALL TICKERS COMBINED  |  {grand_trades} trade(s)  |  Wins: {grand_wins}/{grand_trades}  |  Max concurrent positions: {self._max_concurrent}")
            self._ol(lines, f"  Combined PnL: ${grand_total:+,.2f}  |  Avg PnL/trade: ${grand_total / grand_trades:+,.2f}" if grand_trades else f"  Combined PnL: ${grand_total:+,.2f}")
            self._ol(lines, f"  Total Fees:   ${self._total_fees:,.2f}  |  PnL net of fees: ${grand_total - self._total_fees:+,.2f}")
            self._ol(lines, f"  SKIP TOTALS: {grand_attempts} attempted | {grand_trades} traded | {grand_skipped} skipped (no_pair={grand_no_pair}, low_debit={grand_low_debit}, other={grand_other_skip})")
            self._ol(lines, f"{'='*80}")

        # ── Persist full log to ObjectStore (no 100 KB cap) ──────────────
        # _all_lines has EVERY log line (ENTRY, EXIT, HEDGE, etc.) with timestamps
        self.ObjectStore.Save("backtest_logs", "\n".join(self._all_lines))
