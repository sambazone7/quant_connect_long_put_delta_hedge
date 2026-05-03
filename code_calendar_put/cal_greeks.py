# region imports
from AlgorithmImports import *
from collections import deque
from datetime import timedelta
import math
from scipy.stats import norm
from cal_config import (
    IV_HARD_MIN, IV_HARD_MAX, IV_JUMP_REL,
    IV_SPREAD_REL_MAX, IV_SPREAD_ABS_MAX, IV_VEGA_MIN,
)
# endregion

"""
cal_greeks.py — Custom Black-Scholes greeks + per-leg IV smoothing.

Used by multi_ticker_qc_earnings_calendar_put.py inside _delta_hedge when
cal_config.COMPUTE_OWN_GREEKS is True. Decouples hedge greeks from QC's
chain-derived c.Greeks.* (which inherit IV-inversion noise on degenerate
short-expiry options).

Two parts:

  - bs_put_greeks(...): closed-form European Black-Scholes put greeks.
    Caveat: US equity options are American — BS underestimates American
    put values, especially deep-ITM on dividend-paying stocks. For typical
    earnings-calendar use (mostly near-ATM, short hold), error <3% on greeks.

  - IVSmoother: per-leg rolling-window IV smoother with outlier rejection.
    Long and short legs are kept in fully separate instances and never
    mingled. Sampling cadence is per-bar (~390 samples/day for minute
    resolution); valid samples within the last `window_days` calendar
    days are equally-weighted in the average.
"""

__all__ = [
    "bs_put_greeks", "IVSmoother",
    "_get_risk_free_rate", "_get_dividend_yield", "_sample_iv_for_smoothers",
]


def bs_put_greeks(S, K, T, r, q, sigma):
    """
    European Black-Scholes greeks for a put option.

    Inputs (all in standard units):
        S     : underlying spot price
        K     : strike
        T     : time to expiry in YEARS (e.g. 7/365.25)
        r     : continuously-compounded risk-free rate (e.g. 0.045)
        q     : continuous dividend yield (e.g. 0.0)
        sigma : implied volatility (annualized, e.g. 0.30 for 30%)

    Returns dict with:
        delta : per-share, signed (negative for puts, in [-e^(-qT), 0])
        gamma : per-share, positive
        theta : per-DAY (calendar) — total theta_year / 365
        vega  : per 1% IV change (i.e. theta_per_volpoint × 0.01)
    """
    # Defensive clamps — handle expired contracts and zero-vol edge cases
    T     = max(T, 1.0 / (365.0 * 24.0))   # min 1 hour to avoid div-by-zero
    sigma = max(sigma, 0.01)               # min 1% vol
    if S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    nd1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)   # N'(d1)

    delta = -math.exp(-q * T) * norm.cdf(-d1)
    gamma = math.exp(-q * T) * nd1 / (S * sigma * sqrtT)
    vega  = S * math.exp(-q * T) * nd1 * sqrtT * 0.01           # per 1% IV
    theta = (
        -S * nd1 * sigma * math.exp(-q * T) / (2.0 * sqrtT)
        + q * S * norm.cdf(-d1) * math.exp(-q * T)
        - r * K * math.exp(-r * T) * norm.cdf(-d2)
    ) / 365.0

    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}


class IVSmoother:
    """
    Per-leg IV smoother with outlier rejection.

    Accepts every bar's raw IV via update(...). Valid samples are appended
    to a rolling window; samples older than `window_days` are pruned on
    each update. The smoothed value is the unweighted mean of all valid
    samples currently in the window.

    Long and short legs MUST use separate instances — this class holds no
    state about other legs and never falls back to another leg's IV.
    """

    def __init__(self, window_days=5):
        self.window_days   = window_days
        self.history       = deque()   # (timestamp, iv) tuples — oldest first
        self.last_raw      = None
        self.last_rejected = False
        self.reject_counts = {}        # reason_prefix → count
        self.accept_count  = 0

    # ── Public API ────────────────────────────────────────────────────────

    def seed(self, raw_iv, now_ts):
        """
        Trade-entry seed (no outlier check — entry filters already validated
        the IV by the time this is called).
        """
        if raw_iv is not None and raw_iv > 0:
            self.history.append((now_ts, float(raw_iv)))
            self.accept_count += 1

    def update(self, raw_iv, now_ts, *,
               bid=None, ask=None, vega=None,
               hard_min=0.03, hard_max=3.0, jump_rel=0.5,
               spread_rel_max=0.5, spread_abs_max=0.3, vega_min=0.01):
        """
        Try to record one sample. Returns (smoothed, rejected, reason).

        - Always prunes samples older than `window_days` calendar days first.
        - If outlier check rejects → buffer unchanged, smoothed unchanged.
        - If accepted → appended to buffer.
        """
        self._prune(now_ts)

        if raw_iv is None:
            return self.current_smooth(), True, "none"

        rejected, reason = self._check_outlier(
            float(raw_iv), bid, ask, vega,
            hard_min, hard_max, jump_rel,
            spread_rel_max, spread_abs_max, vega_min,
        )

        self.last_raw      = float(raw_iv)
        self.last_rejected = rejected

        if rejected:
            key = reason.split("(")[0] if reason else "unknown"
            self.reject_counts[key] = self.reject_counts.get(key, 0) + 1
        else:
            self.history.append((now_ts, float(raw_iv)))
            self.accept_count += 1

        return self.current_smooth(), rejected, reason

    def current_smooth(self):
        """Unweighted mean of valid samples currently in the window. None if empty."""
        if not self.history:
            return None
        return sum(iv for (_, iv) in self.history) / len(self.history)

    def sample_count(self):
        return len(self.history)

    def reject_summary(self):
        """One-line diagnostic: cumulative accept count + reject counts by reason."""
        parts = [f"accepted={self.accept_count}", f"window={len(self.history)}"]
        for k, v in self.reject_counts.items():
            parts.append(f"{k}={v}")
        return " ".join(parts)

    # ── Internals ─────────────────────────────────────────────────────────

    def _prune(self, now_ts):
        """Drop samples older than `window_days` calendar days from `now_ts`."""
        cutoff = now_ts - timedelta(days=self.window_days)
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    def _check_outlier(self, raw_iv, bid, ask, vega,
                       hard_min, hard_max, jump_rel,
                       spread_rel_max, spread_abs_max, vega_min):
        # 1. Hard band — catches NaN-equivalents and runaway inversions
        if not (hard_min <= raw_iv <= hard_max):
            return True, f"hard_band({raw_iv:.3f})"

        # 2. Quote-spread quality — wide spreads make mid (and IV inversion) unreliable
        if bid is not None and ask is not None and bid > 0 and ask >= bid:
            mid = (bid + ask) / 2.0
            spread = ask - bid
            if mid > 0 and (spread / mid) > spread_rel_max:
                return True, f"spread_rel({spread/mid:.2f})"
            if spread > spread_abs_max:
                return True, f"spread_abs({spread:.2f})"

        # 3. Bar-to-bar jump — catches transient quote spikes
        last = self.current_smooth()
        if last is not None and last > 0:
            denom = max(last, 0.10)
            if abs(raw_iv - last) / denom > jump_rel:
                return True, f"jump({raw_iv:.3f}_vs_{last:.3f})"

        # 4. Low vega — IV inversion is mathematically unstable here
        if vega is not None and vega < vega_min:
            return True, f"low_vega({vega:.4f})"

        return False, None


# ─── Algorithm-bound helpers (assigned as methods on the algo class) ──────────
# Each takes `self` (the algorithm instance) as first arg, so they work
# identically to regular methods when bound via class attribute assignment.


def _get_risk_free_rate(self):
    """Current annualized risk-free rate from QC's interest-rate model.
    Fallback to 0.045 (typical recent 3M T-bill) if unavailable."""
    if self._risk_free_model is None:
        return 0.045
    try:
        return float(self._risk_free_model.GetInterestRate(self.Time))
    except Exception:
        return 0.045


def _get_dividend_yield(self, ticker):
    """Cached dividend yield for `ticker` (refreshed weekly).
    Returns 0.0 if Fundamentals data unavailable.

    QC's ValuationRatios exposes several dividend-yield fields and the
    actual attribute name has varied across QC versions. We try the most
    likely names in order and silently swallow AttributeError on each."""
    cached = self._dividend_yields.get(ticker)
    today = self.Time.date()
    if cached is not None and (today - cached[1]).days <= 7:
        return cached[0]

    y = 0.0
    try:
        vr = self.Securities[ticker].Fundamentals.ValuationRatios
        # Try the most-common attribute names in order; first non-None wins.
        for attr in ("ForwardDividendYield", "TrailingDividendYield",
                     "DivYield5Year", "DividendYield"):
            try:
                v = getattr(vr, attr, None)
            except Exception:
                v = None
            if v is not None and v > 0:
                y = float(v)
                break
    except Exception:
        y = 0.0

    if y is None or y < 0:
        y = 0.0
    self._dividend_yields[ticker] = (float(y), today)
    return float(y)


def _sample_iv_for_smoothers(self, ticker, ts):
    """Pull raw IV/bid/ask/vega per leg from the latest chain and update
    each leg's IVSmoother. Called from OnData every bar an ACTIVE position
    has a refreshed chain. Long and short smoothers are independent."""
    long_sym  = ts.get("put_symbol")
    short_sym = ts.get("short_put_symbol")
    if long_sym is None or short_sym is None or ts.get("chain") is None:
        return

    long_raw = short_raw = None
    long_bid = long_ask = short_bid = short_ask = None
    long_qc_vega = short_qc_vega = None

    for c in ts["chain"]:
        if c.Symbol == long_sym:
            try:    long_raw = c.ImpliedVolatility
            except Exception: pass
            try:
                long_bid     = c.BidPrice
                long_ask     = c.AskPrice
                long_qc_vega = c.Greeks.Vega
            except Exception: pass
        elif c.Symbol == short_sym:
            try:    short_raw = c.ImpliedVolatility
            except Exception: pass
            try:
                short_bid     = c.BidPrice
                short_ask     = c.AskPrice
                short_qc_vega = c.Greeks.Vega
            except Exception: pass

    now = self.Time
    if long_raw is not None and long_raw > 0 and ts.get("long_iv_smoother") is not None:
        ts["long_iv_smoother"].update(
            long_raw, now,
            bid=long_bid, ask=long_ask, vega=long_qc_vega,
            hard_min=IV_HARD_MIN, hard_max=IV_HARD_MAX, jump_rel=IV_JUMP_REL,
            spread_rel_max=IV_SPREAD_REL_MAX, spread_abs_max=IV_SPREAD_ABS_MAX,
            vega_min=IV_VEGA_MIN,
        )
    if short_raw is not None and short_raw > 0 and ts.get("short_iv_smoother") is not None:
        ts["short_iv_smoother"].update(
            short_raw, now,
            bid=short_bid, ask=short_ask, vega=short_qc_vega,
            hard_min=IV_HARD_MIN, hard_max=IV_HARD_MAX, jump_rel=IV_JUMP_REL,
            spread_rel_max=IV_SPREAD_REL_MAX, spread_abs_max=IV_SPREAD_ABS_MAX,
            vega_min=IV_VEGA_MIN,
        )
