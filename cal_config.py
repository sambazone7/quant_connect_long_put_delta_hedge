# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import requests
# endregion

# Ensure import * exports underscore-prefixed names too
__all__ = [
    "N", "K", "S", "MIN_NET_DEBIT", "SPREAD_CUTOFF_PCT", "DELTA_HEDGE",
    "HEDGE_MODE", "PNL_TOLERANCE", "THETA_K", "MIN_TOLERANCE", "DRIFT_FLOOR",
    "D_mult", "RV_SIGMA", "Z",
    "MAX_PUT_PCT", "PUT_LIMIT_MULT", "N_WEEKLY_AFTER_EARNINGS", "MAX_SHORT_EARN_DAYS", "PRICE_MODEL",
    "HOURLY_BARS", "TRADE_TIME_MIN", "HEDGE_TIME_MIN", "EXIT_DAYS_BEFORE",
    "FMP_API_KEY", "MANUAL_EARNINGS_DATES",
    "_fetch_earnings_fmp", "_mid",
    "NullAssignmentModel", "MidPriceFillModel",
]

# ─── Configurable Parameters ──────────────────────────────────────────────────

N      = 16       # Number of past earnings events per ticker (most recent N)
K      = 25       # Fixed entry day (trading days before earnings)
S      = 10_000   # Notional USD value of calendar spread at entry (per ticker)
                  # Sized by net debit: n_contracts = S / (net_debit × 100)
MIN_NET_DEBIT = 0.75   # Skip trade if long_mid - short_mid < this (dollars)
SPREAD_CUTOFF_PCT = 0.0   # Max bid-ask spread as fraction of option mid price (0.20 = 20%)
                          # Skip entry if either leg exceeds this.  0 = disabled.
DELTA_HEDGE = True   # True → delta-hedge with stock daily;  False → no stock hedging
HEDGE_MODE    = "theta"  # "gamma" → fixed PnL-tolerance trigger (ΔS = √(2·tol/Γ))
                         # "theta" → theta-scaled PnL trigger (tol = THETA_K × |daily θ|)
                         # "sigma" → original vol-scaled delta tolerance
PNL_TOLERANCE = 100      # (gamma mode) Dollar P&L threshold per position before re-hedging
THETA_K       = 1.0      # (theta mode) Scalar on daily theta: tol = THETA_K × |θ_daily_position|
MIN_TOLERANCE = 50       # (theta mode) Floor on dynamic tolerance to guard against near-zero theta
DRIFT_FLOOR   = 0.10     # (gamma/theta) Max |position_delta| as fraction of option exposure (0.10 = 10%)
D_mult  = 1.0    # (sigma mode) tolerance = D_mult × daily_sigma_frac × |option_exposure|
RV_SIGMA = True   # (sigma mode) True → 30d realized vol; False → long put's live IV
Z      = 0.0      # IV/RV filter: skip entry if IV/RV >= Z  (0.0 = disabled)
MAX_PUT_PCT = 0.15  # Sanity: skip entry if long_put_mid > stock_price × MAX_PUT_PCT
PUT_LIMIT_MULT = 1.2  # Limit order for long put at long_mid × this (prevents bad fills)
N_WEEKLY_AFTER_EARNINGS = 1  # Which weekly expiry after earnings for the long put:
                             # 1 → first weekly after earnings, 2 → second weekly, etc.
MAX_SHORT_EARN_DAYS = 7      # Max calendar days between short put expiry and earnings date
                             # If short expiry is further than this from earnings → skip trade
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
EXIT_DAYS_BEFORE = 1      # Trading days before short put expiry to close position
                          # 1 → close day before expiry,  2 → two days before, etc.

# ─── Financial Modeling Prep API ──────────────────────────────────────────────
FMP_API_KEY = ""   # Leave empty to rely solely on MANUAL_EARNINGS_DATES below

# ─── Earnings Dates ───────────────────────────────────────────────────────────
# Imported from tickerlist.py — add one key per ticker there.
from tickerlist_small import MANUAL_EARNINGS_DATES

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


# ─── Utility classes ─────────────────────────────────────────────────────────

class NullAssignmentModel(DefaultOptionAssignmentModel):
    """Never trigger automatic option assignment — the algo manages all exits."""
    def get_assignment(self, parameters):
        return OptionAssignmentResult.NULL


class MidPriceFillModel(ImmediateFillModel):
    def market_fill(self, asset, order):
        fill = super().market_fill(asset, order)
        mid  = _mid(asset.bid_price, asset.ask_price)
        if mid > 0:
            fill.fill_price = round(mid, 2)
        return fill

    def limit_fill(self, asset, order):
        mid = _mid(asset.bid_price, asset.ask_price)
        if mid <= 0:
            return super().limit_fill(asset, order)
        _no_fill = OrderStatus.NONE
        # Buy limit: fill at mid only if mid <= limit price
        if order.quantity > 0 and mid > order.limit_price:
            return OrderEvent(order.id, order.symbol, asset.local_time,
                              _no_fill, order.direction,
                              0, 0, OrderFee.ZERO, "mid above limit")
        # Sell limit: fill at mid only if mid >= limit price
        if order.quantity < 0 and mid < order.limit_price:
            return OrderEvent(order.id, order.symbol, asset.local_time,
                              _no_fill, order.direction,
                              0, 0, OrderFee.ZERO, "mid below limit")
        # Condition met — fill at mid
        fill = super().limit_fill(asset, order)
        if fill.status == OrderStatus.FILLED:
            fill.fill_price = round(mid, 2)
        return fill
