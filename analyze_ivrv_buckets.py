import csv, statistics

rows = []
with open("logs-qqq/parsed-march28-15days.csv") as f:
    for r in csv.DictReader(f):
        ivrv_raw = r.get("iv_rv", "").strip()
        sim_raw = r.get("sim_pnl", "").strip().replace(",", "").replace("+", "")
        if not ivrv_raw or ivrv_raw in ("n/a", ""):
            continue
        try:
            ivrv = float(ivrv_raw)
            sim = float(sim_raw)
        except Exception:
            continue
        rows.append((ivrv, sim))

print(f"Total trades with valid IV/RV + sim_pnl: {len(rows)}")
print()

bucket_defs = [
    ("IV/RV < 0.80", lambda x: x < 0.80),
    ("IV/RV 0.80-1.00", lambda x: 0.80 <= x < 1.00),
    ("IV/RV 1.00-1.20", lambda x: 1.00 <= x < 1.20),
    ("IV/RV 1.20-1.50", lambda x: 1.20 <= x < 1.50),
    ("IV/RV 1.50+", lambda x: x >= 1.50),
]

buckets = {name: [] for name, _ in bucket_defs}
for ivrv, pnl in rows:
    for name, fn in bucket_defs:
        if fn(ivrv):
            buckets[name].append((ivrv, pnl))
            break

hdr = (
    f"{'Bucket':<18} {'Trades':>6} {'Win%':>6} {'Total PnL':>14} "
    f"{'Avg PnL':>10} {'Med PnL':>10} {'Avg IV/RV':>10} "
    f"{'Min PnL':>10} {'Max PnL':>10} {'StdDev':>10}"
)
print(hdr)
print("-" * len(hdr))

for name, _ in bucket_defs:
    data = buckets[name]
    if not data:
        print(f"{name:<18} {'(no trades)':>6}")
        continue
    pnls = [d[1] for d in data]
    ivrvs = [d[0] for d in data]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    avg = total / n
    med = statistics.median(pnls)
    mn = min(pnls)
    mx = max(pnls)
    sd = statistics.stdev(pnls) if n > 1 else 0
    avg_ivrv = sum(ivrvs) / n
    print(
        f"{name:<18} {n:>6} {wins/n*100:>5.1f}% {total:>+14,.2f} "
        f"{avg:>+10,.2f} {med:>+10,.2f} {avg_ivrv:>9.2f} "
        f"{mn:>+10,.2f} {mx:>+10,.2f} {sd:>10,.2f}"
    )

print()
print("=== Win/Loss breakdown per bucket ===")
hdr2 = f"{'Bucket':<18} {'Avg Win':>10} {'Avg Loss':>10} {'W/L Ratio':>10}"
print(hdr2)
print("-" * len(hdr2))
for name, _ in bucket_defs:
    data = buckets[name]
    if not data:
        continue
    pnls = [d[1] for d in data]
    wins_pnl = [p for p in pnls if p > 0]
    loss_pnl = [p for p in pnls if p <= 0]
    avg_w = sum(wins_pnl) / len(wins_pnl) if wins_pnl else 0
    avg_l = sum(loss_pnl) / len(loss_pnl) if loss_pnl else 0
    ratio = abs(avg_w / avg_l) if avg_l != 0 else float("inf")
    print(f"{name:<18} {avg_w:>+10,.2f} {avg_l:>+10,.2f} {ratio:>10.2f}")

# Pearson correlation
n = len(rows)
xs = [r[0] for r in rows]
ys = [r[1] for r in rows]
mx_v = sum(xs) / n
my_v = sum(ys) / n
cov = sum((xs[i] - mx_v) * (ys[i] - my_v) for i in range(n)) / (n - 1)
sx = statistics.stdev(xs)
sy = statistics.stdev(ys)
corr = cov / (sx * sy) if sx > 0 and sy > 0 else 0
print()
print(f"Pearson correlation (IV/RV vs sim_pnl): r = {corr:.4f}")
print("  (Values near 0 = no linear correlation, near +1/-1 = strong)")
