# region imports
from AlgorithmImports import *
from datetime import timedelta, date as Date
import requests
# endregion

# Ensure import * exports underscore-prefixed names too
__all__ = [
    "N", "K", "S", "MIN_NET_DEBIT", "SPREAD_CUTOFF_PCT", "DELTA_HEDGE", "D_mult", "RV_SIGMA", "Z",
    "MAX_PUT_PCT", "PUT_LIMIT_MULT", "MAX_SPREAD_DAYS", "PRICE_MODEL",
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
D_mult  = 1.0    # Delta-tolerance scalar: tolerance = D_mult × daily_sigma_frac × |option_exposure|
                  # e.g. 1.0 → tolerate up to 1 daily-sigma of delta drift before re-hedging
RV_SIGMA = True   # True  → hedge tolerance sigma from live 30-day realized vol (refreshed daily)
                  # False → hedge tolerance sigma from long put's live IV (fallback: entry IV)
Z      = 0.0      # IV/RV filter: skip entry if IV/RV >= Z  (0.0 = disabled)
MAX_PUT_PCT = 0.15  # Sanity: skip entry if long_put_mid > stock_price × MAX_PUT_PCT
PUT_LIMIT_MULT = 1.2  # Limit order for long put at long_mid × this (prevents bad fills)
MAX_SPREAD_DAYS = 7   # Max calendar days between short and long put expirations
                      # If wider → skip trade (no weekly expirations available)
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
        # Buy limit: fill at mid only if mid <= limit price
        if order.Quantity > 0 and mid > order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid above limit")
        # Sell limit: fill at mid only if mid >= limit price
        if order.Quantity < 0 and mid < order.LimitPrice:
            return OrderEvent(order.Id, order.Symbol, asset.LocalTime,
                              _no_fill, order.Direction,
                              0, 0, OrderFee.Zero, "mid below limit")
        # Condition met — fill at mid
        fill = super().LimitFill(asset, order)
        if fill.Status == OrderStatus.Filled:
            fill.FillPrice = round(mid, 2)
        return fill
