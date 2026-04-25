# region imports
from AlgorithmImports import *
# endregion

# ─── Standalone functions assigned as methods on the algorithm class ──────────
# Each takes `self` (the algorithm instance) as first arg, so they work
# identically to regular methods when assigned via class attribute in
# multi_ticker_qc_earnings_long_put_v4.py.
#
# Extracted out of the main algo file to keep the QC source under the 64KB
# per-file limit.


def _mid(bid, ask):
    """Mid-price helper. Returns 0.0 for degenerate quotes (signal to caller
    that the quote can't be trusted)."""
    if bid > 0 and ask >= bid:
        return (bid + ask) / 2.0
    return 0.0


# ── Final exit (called from EXITING-state handler in _delta_hedge) ───────────

def _finalize_exit(self, ticker):
    """Called at the 3:45 PM hedge tick once all exit fills are guaranteed in
    (any unfilled limit has already been cancelled and market-flattened by
    the EXITING-state handler in _delta_hedge). Computes PnL from actual
    fills, writes the trade-log row, and resets.
    """
    ts = self._ts[ticker]
    if ts["state"] != "EXITING":
        return  # nothing to finalize

    n_contracts = ts.get("exit_n_contracts", ts["put_contracts"])
    put_exit_iv = ts.get("exit_put_iv",     0.0)

    # PnL from actual fill prices (set by OnOrderEvent as fills arrived)
    put_pnl   = (ts["put_exit_fill"] - ts["put_entry_fill"]) * n_contracts * 100
    stk_pnl   = ts["stock_realized"]
    total_pnl = put_pnl + stk_pnl

    # ── Sim PnL (fair-value put exit + stock hedge PnL) ──────────────
    s_price = self.Securities[ts["stock_symbol"]].Price
    strike  = self.Securities[ts["put_symbol"]].Symbol.ID.StrikePrice
    _put_sec = self.Securities[ts["put_symbol"]]
    call_sym = ts.get("call_symbol")

    if s_price >= strike:
        effective_exit = _mid(_put_sec.BidPrice, _put_sec.AskPrice)
        if effective_exit <= 0:
            # OTM long put with degenerate quote → assume worthless.
            #
            # We do NOT attempt put-call parity recovery here. Both the
            # put and the call on the same strike/expiry come from the
            # SAME QC option chain bar at the SAME timestamp. If the
            # put's mid is degenerate (bid=0/ask=0, one-sided, inverted),
            # the call's mid pulled from the same bar is overwhelmingly
            # likely to be degenerate too — failure modes are correlated
            # at the chain-bar level. Parity recovery on co-degenerate
            # quotes adds noise without information; setting to 0 is the
            # honest answer for missing data.
            #
            # (The ITM branch below DOES still use parity, because there
            # the put's intrinsic value (strike - s_price) is real money
            # independent of the chain quotes, and parity only adds a
            # small time-value adjustment from the call's mid.)
            effective_exit = 0.0
            self.Log(f"  [{ticker}] WARN sim_pnl: OTM put quote degenerate "
                     f"bid={_put_sec.BidPrice:.2f} ask={_put_sec.AskPrice:.2f} "
                     f"— assuming effective_exit=0")
    else:
        call_mid = 0.0
        if call_sym and self.Securities.ContainsKey(call_sym):
            _call_sec = self.Securities[call_sym]
            call_mid = _mid(_call_sec.BidPrice, _call_sec.AskPrice)
            ts["call_spread_exit"] = round(abs(_call_sec.BidPrice - _call_sec.AskPrice) * 100 * n_contracts)
            if call_mid <= 0:
                self.Log(f"  [{ticker}] WARN sim_pnl: call quote degenerate "
                        f"bid={_call_sec.BidPrice:.2f} ask={_call_sec.AskPrice:.2f} — "
                        f"long put treated as pure intrinsic")
        else:
            self.Log(f"  [{ticker}] WARN sim_pnl: call not subscribed at exit — "
                    f"long put treated as pure intrinsic")
        effective_exit = (strike - s_price) + call_mid

    # Allow effective_exit == 0 (worthless put is a valid sim outcome).
    # Only skip if entry fill is missing (no reference price for PnL).
    if ts["put_entry_fill"] > 0:
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

    self.Log(f"  [{ticker}] EXIT FINALIZED: PutPnL=${put_pnl:+,.2f} "
             f"StkPnL=${stk_pnl:+,.2f} Total=${total_pnl:+,.2f}")

    ts["hedge_count"] = 0
    ts["traded_earnings"].add(ed)
    self._reset(ticker)


# ── Emergency exit (stock split from OnData — deferred) ──────────────────────

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
