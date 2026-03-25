import re, sys

def parse(path):
    trades = []
    ticker = None
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r'\s+(\w+) SUMMARY', line)
            if m:
                ticker = m.group(1)
                continue
            m = re.match(
                r'\s+\[([+-])\]\s+'
                r'(\d{4}-\d{2}-\d{2})\s+(\d{4}-\d{2}-\d{2})\s+days=(\d+)'
                r'\s+PutPnL=\$([+\-\d,.]+)'
                r'\s+StkPnL=\$([+\-\d,.]+)'
                r'\s+Stk=([+\-\d.]+)%'
                r'\s+PnL=\$([+\-\d,.]+)'
                r'\s+IVR=(\d+)'
                r'\s+IV/RV=([0-9.n/a]+)',
                line)
            if m:
                pnl = float(m.group(8).replace(",", ""))
                ivr = int(m.group(9))
                ivrv_str = m.group(10)
                ivrv = float(ivrv_str) if ivrv_str != "n/a" else None
                trades.append({
                    "ticker": ticker,
                    "entry": m.group(2),
                    "exit": m.group(3),
                    "days": int(m.group(4)),
                    "pnl": pnl,
                    "ivr": ivr,
                    "ivrv": ivrv,
                    "win": pnl >= 0,
                })
    return trades

def stats(trades, label):
    if not trades:
        print(f"  {label:<45s}  -- no trades --")
        return
    n = len(trades)
    wins = sum(1 for t in trades if t["win"])
    total = sum(t["pnl"] for t in trades)
    avg = total / n
    wp = wins / n * 100
    D = "$"
    print(f"  {label:<45s}  N={n:>4}  Avg={D}{avg:>+9,.0f}  Tot={D}{total:>+12,.0f}  Win%={wp:>5.1f}%")

trades = parse(sys.argv[1])
print(f"Loaded {len(trades)} trades\n")

print("=" * 100)
print("  BASELINE")
print("=" * 100)
stats(trades, "All trades")
print()

print("=" * 100)
print("  BY IV RANK BUCKET")
print("=" * 100)
stats([t for t in trades if t["ivr"] < 55],           "IVR < 55")
stats([t for t in trades if 55 <= t["ivr"] < 65],     "IVR 55-64")
stats([t for t in trades if 65 <= t["ivr"] < 75],     "IVR 65-74")
stats([t for t in trades if 75 <= t["ivr"] < 85],     "IVR 75-84")
stats([t for t in trades if t["ivr"] >= 85],           "IVR >= 85")
print()
stats([t for t in trades if t["ivr"] >= 60],           "IVR >= 60")
stats([t for t in trades if t["ivr"] >= 70],           "IVR >= 70")
stats([t for t in trades if t["ivr"] >= 80],           "IVR >= 80")
print()

print("=" * 100)
print("  BY IV/RV BUCKET")
print("=" * 100)
ivrv_trades = [t for t in trades if t["ivrv"] is not None]
stats([t for t in ivrv_trades if t["ivrv"] < 0.80],           "IV/RV < 0.80")
stats([t for t in ivrv_trades if 0.80 <= t["ivrv"] < 1.00],   "IV/RV 0.80-0.99")
stats([t for t in ivrv_trades if 1.00 <= t["ivrv"] < 1.20],   "IV/RV 1.00-1.19")
stats([t for t in ivrv_trades if 1.20 <= t["ivrv"] < 1.50],   "IV/RV 1.20-1.49")
stats([t for t in ivrv_trades if t["ivrv"] >= 1.50],           "IV/RV >= 1.50")
print()

print("=" * 100)
print("  IV RANK + IV/RV COMBINATIONS")
print("=" * 100)
stats([t for t in ivrv_trades if t["ivr"] >= 70 and t["ivrv"] < 1.0],     "IVR>=70 + IV/RV<1.0")
stats([t for t in ivrv_trades if t["ivr"] >= 70 and t["ivrv"] < 1.2],     "IVR>=70 + IV/RV<1.2")
stats([t for t in ivrv_trades if t["ivr"] >= 70 and t["ivrv"] >= 1.2],    "IVR>=70 + IV/RV>=1.2")
stats([t for t in ivrv_trades if t["ivr"] >= 80 and t["ivrv"] < 1.0],     "IVR>=80 + IV/RV<1.0")
stats([t for t in ivrv_trades if t["ivr"] >= 80 and t["ivrv"] < 1.2],     "IVR>=80 + IV/RV<1.2")
stats([t for t in ivrv_trades if t["ivr"] >= 80 and t["ivrv"] >= 1.2],    "IVR>=80 + IV/RV>=1.2")
print()

print("=" * 100)
print("  PER-TICKER SUMMARY")
print("=" * 100)
tickers = sorted(set(t["ticker"] for t in trades))
for tk in tickers:
    tt = [t for t in trades if t["ticker"] == tk]
    stats(tt, tk)
