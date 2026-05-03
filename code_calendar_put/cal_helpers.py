# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import math
from cal_config import N, FMP_API_KEY, MANUAL_EARNINGS_DATES, _fetch_earnings_fmp
# endregion

"""
cal_helpers.py — Utility helpers bound as methods on the algorithm class.

Each function takes `self` (the algorithm instance) as first arg, so they
work identically to regular methods when assigned via class attribute
binding (same pattern as cal_exit_handlers.py and cal_greeks.py).

Migrated here from multi_ticker_qc_earnings_calendar_put.py to keep the
main file under QuantConnect's 64,000-character per-file limit.
"""

__all__ = [
    "_load_earnings_dates", "_log_earnings_dates",
    "_calc_realized_vol", "_get_vix",
    "_offset_trading_days", "_reset",
    "_log", "_ol", "OnEndOfAlgorithm",
]


def _load_earnings_dates(self, ticker):
    """Load earnings dates for `ticker` from manual list + optional FMP API."""
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


def _get_vix(self):
    """Fetch most recent VIX close via History() with 5-day lookback."""
    try:
        h = self.History(self._vix_symbol, 5, Resolution.Daily)
        if not h.empty:
            return float(h["close"].iloc[-1])
    except Exception:
        pass
    return None


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


def _reset(self, ticker):
    """Reset per-ticker state to FLAT after a trade closes (or aborts)."""
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
    ts["short_put_order_ticket"] = None
    ts["long_exit_ticket"]    = None
    ts["short_exit_ticket"]   = None
    ts["pending_short_put"]   = False
    ts["pending_hedge_shares"]= 0
    ts["entry_aborted"]       = False
    ts["exit_n_contracts"]    = 0
    ts["exit_n_shares"]       = 0
    ts["exit_put_iv"]         = 0.0
    ts["stock_qty"]           = 0
    ts["stock_cost_basis"]    = 0.0
    ts["stock_realized"]      = 0.0
    ts["last_hedge_price"]    = 0.0
    ts["stock_entry_price"]   = 0.0
    ts["stock_max_up_pct"]    = 0.0
    ts["stock_max_dn_pct"]    = 0.0
    ts["entry_earnings"]      = None
    ts["entry_rv"]            = 0.0
    ts["vix_entry"]           = None
    ts["vix_exit"]            = None
    ts["force_exited"]        = False
    ts["_closing_forced"]     = False
    ts["hedge_count"]         = 0
    # Note: orphan_cleaned is NOT reset here — it stays True until
    # _enter_position clears it, preventing repeated cleanup in FLAT state.
    ts["total_fees"]          = 0.0
    ts["call_symbol_long"]    = None
    ts["call_symbol_short"]   = None
    ts["long_spread_entry"]   = 0
    ts["short_spread_entry"]  = 0
    ts["long_spread_exit"]    = 0
    ts["short_spread_exit"]   = 0
    ts["long_iv_smoother"]    = None
    ts["short_iv_smoother"]   = None


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
    grand_hedges = 0
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

        totals = {"long": 0.0, "short": 0.0, "stk": 0.0, "total": 0.0, "sim": 0.0}
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
            f" {'MaxUp%':>8}"
            f" {'MaxDn%':>8}"
            f" {'Stk Chg%':>9}"
            f" {'Combined':>12}"
            f" {'SimPnL':>12}"
            f" {'VIXen':>6} {'VIXex':>6}"
            f" {'IVLen':>9}"
            f" {'IVSen':>9}"
            f" {'IVLex':>8}"
            f" {'IVspread':>9}"
            f" {'ShIV/RV':>8}"
            f" {'IV chg':>7}"
            f" {'IV/RV':>6}"
            f" {'LSpEn':>7}"
            f" {'SSpEn':>7}"
            f" {'LSpEx':>7}"
            f" {'SSpEx':>7}"
            f" {'SPen':>8}"
            f" {'SPex':>8}"
            f" {'LPen':>8}"
            f" {'LPex':>8}"
            f" {'nCal':>5}"
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
            _sim  = t.get("sim_pnl", 0.0)
            _vixen = t.get("vix_entry")
            _vixxs = t.get("vix_exit")
            _vixen_s = f"{_vixen:.1f}" if _vixen else "n/a"
            _vixxs_s = f"{_vixxs:.1f}" if _vixxs else "n/a"
            _lse = t.get("long_spread_entry", 0)
            _sse = t.get("short_spread_entry", 0)
            _lsx = t.get("long_spread_exit", 0)
            _ssx = t.get("short_spread_exit", 0)
            _hcnt = t.get("hedge_count", 0)
            _spen = t.get("short_put_entry_px", 0.0)
            _spex = t.get("short_put_exit_px", 0.0)
            _lpen = t.get("long_put_entry_px", 0.0)
            _lpex = t.get("long_put_exit_px", 0.0)
            self._ol(lines,
                f"  {tag} {t['earnings']!s:<11}"
                f"  {nc:>5}"
                f"  ${t['long_pnl']:>+10,.2f}"
                f"  ${t['short_pnl']:>+10,.2f}"
                f"  ${t['stk_pnl']:>+10,.2f}"
                f"  {t.get('stk_max_up_pct', 0.0):>+7.1f}%"
                f"  {t.get('stk_max_dn_pct', 0.0):>+7.1f}%"
                f"  {chg:>+7.1f}%"
                f"  ${t['total']:>+10,.2f}"
                f"  ${_sim:>+10,.2f}"
                f"  {_vixen_s:>6} {_vixxs_s:>6}"
                f"  {t['iv_entry']:>8.1%}"
                f"  {t.get('short_iv_entry', 0.0):>8.1%}"
                f"  {t['iv_exit']:>7.1%}"
                f"  {iv_spr:>8.1%}"
                f"  {sh_rv:>8.2f}"
                f"  {iv_chg:>+6.0f}%"
                f"  {ratio:>6}"
                f"  {_lse:>7}"
                f"  {_sse:>7}"
                f"  {_lsx:>7}"
                f"  {_ssx:>7}"
                f"  {_spen:>8.2f}"
                f"  {_spex:>8.2f}"
                f"  {_lpen:>8.2f}"
                f"  {_lpex:>8.2f}"
                f"  {nc:>5}"
                f"  Hdg={_hcnt}"
            )
            totals["long"]  += t["long_pnl"]
            totals["short"] += t["short_pnl"]
            totals["stk"]   += t["stk_pnl"]
            totals["total"] += t["total"]
            totals["sim"]   += _sim

        printed = n - skipped
        avg = totals["total"] / printed if printed > 0 else 0.0
        self._ol(lines, f"  {'-'*160}")
        self._ol(lines,
            f"  {'TOTAL':<15}"
            f"  ${totals['long']:>+10,.2f}"
            f"  ${totals['short']:>+10,.2f}"
            f"  ${totals['stk']:>+10,.2f}"
            f"  {'':>8}"
            f"  {'':>8}"
            f"  {'':>9}"
            f"  ${totals['total']:>+10,.2f}"
            f"  ${totals['sim']:>+10,.2f}"
        )
        ticker_hedges = sum(t.get("hedge_count", 0) for t in valid)
        avg_hedges = ticker_hedges / printed if printed > 0 else 0.0
        self._ol(lines, f"  Avg PnL/trade: ${avg:+,.2f}  |  Hedges: {ticker_hedges} total, {avg_hedges:.1f} avg/trade")
        if self._theta_exits > 0 and len(self._ts) == 1:
            self._ol(lines, f"  Theta exits: {self._theta_exits}")
        self._ol(lines, f"{'='*80}")
        grand_total  += totals["total"]
        grand_hedges += ticker_hedges

    if len(self._ts) > 1:
        grand_skipped = grand_no_pair + grand_low_debit + grand_other_skip
        self._ol(lines, f"{'='*80}")
        self._ol(lines, f"  ALL TICKERS COMBINED  |  {grand_trades} trade(s)  |  Wins: {grand_wins}/{grand_trades}  |  Max concurrent positions: {self._max_concurrent}")
        avg_h = grand_hedges / grand_trades if grand_trades else 0.0
        self._ol(lines, f"  Combined PnL: ${grand_total:+,.2f}  |  Avg PnL/trade: ${grand_total / grand_trades:+,.2f}  |  Hedges: {grand_hedges} total, {avg_h:.1f} avg/trade" if grand_trades else f"  Combined PnL: ${grand_total:+,.2f}")
        self._ol(lines, f"  Total Fees:   ${self._total_fees:,.2f}  |  PnL net of fees: ${grand_total - self._total_fees:+,.2f}")
        self._ol(lines, f"  SKIP TOTALS: {grand_attempts} attempted | {grand_trades} traded | {grand_skipped} skipped (no_pair={grand_no_pair}, low_debit={grand_low_debit}, other={grand_other_skip})")
        if self._theta_exits > 0:
            self._ol(lines, f"  Theta exits: {self._theta_exits}")
        self._ol(lines, f"{'='*80}")

    # ── Persist full log to ObjectStore (no 100 KB cap) ──────────────
    # _all_lines has EVERY log line (ENTRY, EXIT, HEDGE, etc.) with timestamps
    self.ObjectStore.Save("backtest_logs", "\n".join(self._all_lines))

