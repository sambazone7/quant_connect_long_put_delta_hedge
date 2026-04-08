# region imports
from AlgorithmImports import *
# endregion

# ─── Standalone functions assigned as methods on the algorithm class ──────────
# Each takes `self` (the algorithm instance) as first arg, so they work
# identically to regular methods when assigned via class attribute.


def _immediate_close_all(self, ticker, reason):
    """Close ALL remaining legs immediately from inside OnOrderEvent.
    Used when an unexpected fill (assignment / auto-liquidation) is detected
    so that no positions are left orphaned."""
    ts = self._ts[ticker]
    if ts.get("force_exited"):
        return
    ts["force_exited"] = True
    ts["_closing_forced"] = True

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
        "sim_pnl":            0.0,
        "vix_entry":          ts["vix_entry"],
        "vix_exit":           ts["vix_exit"],
        "iv_entry":           _long_iv,
        "iv_exit":            0.0,
        "rv":                 _rv,
        "iv_spread_entry":    _long_iv - _short_iv,
        "short_iv_entry":     _short_iv,
        "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
        "_assign_strike":     short_strike,
        "long_spread_entry":  ts.get("long_spread_entry", 0),
        "short_spread_entry": ts.get("short_spread_entry", 0),
        "long_spread_exit":   0,
        "short_spread_exit":  0,
        "hedge_count":        ts["hedge_count"],
        "short_put_entry_px": ts["short_put_entry_fill"],
        "short_put_exit_px":  ts["short_put_exit_fill"],
        "long_put_entry_px":  ts["put_entry_fill"],
        "long_put_exit_px":   ts["put_exit_fill"],
    })
    ts["traded_earnings"].add(ed)
    self._reset(ticker)


# ── Forced-exit finalization (split: stock+other legs closed at Submitted, ──
# ── MOC fills arrive at 16:00 — finalize P&L when last fill arrives)      ──

def _finalize_forced_exit(self, ticker):
    """Called from put Filled handler when force_exited is True.
    All legs were closed at Submitted time; this just records the
    MOC fill price and finalizes P&L."""
    ts = self._ts[ticker]
    n = ts["put_contracts"]
    long_pnl  = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n * 100
    short_pnl = (ts["short_put_entry_fill"] - ts["short_put_exit_fill"]) * n * 100
    stk_pnl   = ts["stock_realized"]
    total     = long_pnl + short_pnl + stk_pnl

    entry_px = ts["stock_entry_price"]
    exit_px  = self.Securities[ts["stock_symbol"]].Price if entry_px > 0 else 0.0
    stk_chg_pct = ((exit_px - entry_px) / entry_px * 100) if entry_px > 0 else 0.0

    ed = ts["entry_earnings"].date() if ts["entry_earnings"] else self.Time.date()

    _short_iv = ts["short_put_entry_iv"]
    _long_iv  = ts["put_entry_iv"]
    _rv       = ts["entry_rv"]

    ts["trade_log"].append({
        "earnings":           ed,
        "n_contracts":        n,
        "long_pnl":           long_pnl,
        "short_pnl":          short_pnl,
        "stk_pnl":            stk_pnl,
        "stk_chg_pct":        stk_chg_pct,
        "total":              total,
        "sim_pnl":            0.0,
        "vix_entry":          ts["vix_entry"],
        "vix_exit":           ts["vix_exit"],
        "iv_entry":           _long_iv,
        "iv_exit":            0.0,
        "rv":                 _rv,
        "iv_spread_entry":    _long_iv - _short_iv,
        "short_iv_entry":     _short_iv,
        "short_iv_rv":        _short_iv / _rv if _rv > 0 else 0.0,
        "long_spread_entry":  ts.get("long_spread_entry", 0),
        "short_spread_entry": ts.get("short_spread_entry", 0),
        "long_spread_exit":   0,
        "short_spread_exit":  0,
        "hedge_count":        ts["hedge_count"],
        "short_put_entry_px": ts["short_put_entry_fill"],
        "short_put_exit_px":  ts["short_put_exit_fill"],
        "long_put_entry_px":  ts["put_entry_fill"],
        "long_put_exit_px":   ts["put_exit_fill"],
    })
    self._log(f"  [{self.Time.date()}] FORCED EXIT FINALIZED: "
              f"{ticker} longPnL=${long_pnl:+,.0f} shortPnL=${short_pnl:+,.0f} "
              f"stkPnL=${stk_pnl:+,.0f} total=${total:+,.0f}")
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

    ts["_closing_forced"] = True

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
        "sim_pnl":            0.0,
        "vix_entry":          ts["vix_entry"],
        "vix_exit":           ts["vix_exit"],
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
        "hedge_count":        ts["hedge_count"],
        "short_put_entry_px": ts["short_put_entry_fill"],
        "short_put_exit_px":  ts["short_put_exit_fill"],
        "long_put_entry_px":  ts["put_entry_fill"],
        "long_put_exit_px":   ts["put_exit_fill"],
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

            ts["orphan_cleaned"] = True

            # ── Record the assignment stock P&L as a correction to
            #    the last trade_log entry (written by _immediate_close_all
            #    before the assignment stock arrived) ──────────────────
            if ts["trade_log"]:
                last = ts["trade_log"][-1]
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

    # Orphan condition: any mismatch where one/both puts are gone but not properly exited
    is_orphan = False
    if long_qty > 0 and short_qty == 0:
        is_orphan = True   # short was assigned
    elif long_qty == 0 and short_qty < 0:
        is_orphan = True   # long was auto-liquidated
    elif long_qty == 0 and short_qty == 0 and stk_qty != 0:
        is_orphan = True   # both puts gone, stock hedge remains

    if is_orphan:
        self._log(f"  [{self.Time.date()}] ORPHAN DETECTED for {ticker}: "
                 f"long_qty={long_qty}, short_qty={short_qty}, stk_qty={stk_qty} "
                 f"— closing all positions")

        ts["force_exited"] = True
        ts["_closing_forced"] = True
        self._filling_ticker = ticker

        # Close long put if still held
        if long_qty > 0:
            self.MarketOrder(put_sym, -long_qty)

        # Close short put if still held
        if short_qty < 0:
            self.MarketOrder(short_sym, -short_qty)

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
            "sim_pnl":            0.0,
            "vix_entry":          ts["vix_entry"],
            "vix_exit":           ts["vix_exit"],
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
            "hedge_count":        ts["hedge_count"],
            "short_put_entry_px": ts["short_put_entry_fill"],
            "short_put_exit_px":  ts["short_put_exit_fill"],
            "long_put_entry_px":  ts["put_entry_fill"],
            "long_put_exit_px":   ts["put_exit_fill"],
        })
        ts["traded_earnings"].add(ed)
        self._reset(ticker)
