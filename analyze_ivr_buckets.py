import csv, statistics

rows = []
with open("logs-qqq/parsed-march28-15days.csv") as f:
    reader = csv.DictReader(f)
    for r in reader:
        ivr_raw = r.get("ivr", "").strip()
        sim_raw = r.get("sim_pnl", "").strip().replace(",", "").replace("+", "")
        if not ivr_raw or ivr_raw in ("n/a", ""):
            continue
        try:
            ivr_val = float(ivr_raw)
            sim_val = float(sim_raw)
        except Exception:
            continue
        rows.append((ivr_val, sim_val))

print(f"Total trades with valid IVR + sim_pnl: {len(rows)}")
print()

bucket_defs = [
    ("IVR < 20",  lambda x: x < 20),
    ("IVR 20-49", lambda x: 20 <= x < 50),
    ("IVR 50-69", lambda x: 50 <= x < 70),
    ("IVR 70+",   lambda x: x >= 70),
]

buckets = {name: [] for name, _ in bucket_defs}
for ivr, pnl in rows:
    for name, fn in bucket_defs:
        if fn(ivr):
            buckets[name].append((ivr, pnl))
            break

hdr = f"{'Bucket':<14} {'Trades':>6} {'Win%':>6} {'Total PnL':>14} {'Avg PnL':>10} {'Med PnL':>10} {'Avg IVR':>8} {'Min PnL':>10} {'Max PnL':>10} {'StdDev':>10}"
print(hdr)
print("-" * len(hdr))

for name, _ in bucket_defs:
    data = buckets[name]
    if not data:
        print(f"{name:<14} {'(no trades)':>6}")
        continue
    pnls = [d[1] for d in data]
    ivrs = [d[0] for d in data]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    avg = total / n
    med = statistics.median(pnls)
    mn = min(pnls)
    mx = max(pnls)
    sd = statistics.stdev(pnls) if n > 1 else 0
    avg_ivr = sum(ivrs) / n
    print(f"{name:<14} {n:>6} {wins/n*100:>5.1f}% {total:>+14,.2f} {avg:>+10,.2f} {med:>+10,.2f} {avg_ivr:>7.1f} {mn:>+10,.2f} {mx:>+10,.2f} {sd:>10,.2f}")

print()
print("=== Win/Loss breakdown per bucket ===")
hdr2 = f"{'Bucket':<14} {'Avg Win':>10} {'Avg Loss':>10} {'W/L Ratio':>10}"
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
    print(f"{name:<14} {avg_w:>+10,.2f} {avg_l:>+10,.2f} {ratio:>10.2f}")

# Pearson correlation
n = len(rows)
ivrs_all = [r[0] for r in rows]
pnls_all = [r[1] for r in rows]
mean_ivr = sum(ivrs_all) / n
mean_pnl = sum(pnls_all) / n
cov = sum((ivrs_all[i] - mean_ivr) * (pnls_all[i] - mean_pnl) for i in range(n)) / (n - 1)
std_ivr = statistics.stdev(ivrs_all)
std_pnl = statistics.stdev(pnls_all)
corr = cov / (std_ivr * std_pnl) if std_ivr > 0 and std_pnl > 0 else 0
print()
print(f"Pearson correlation (IVR vs sim_pnl): r = {corr:.4f}")
print("  (Values near 0 = no linear correlation, near +1/-1 = strong)")
