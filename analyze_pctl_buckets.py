import csv, statistics

CSV_FILE = "logs-qqq/qc-logout-top15.csv"

# ── Load data ──────────────────────────────────────────────────────────────
rows = []
by_ticker = {}
with open(CSV_FILE) as f:
    for r in csv.DictReader(f):
        pctl_raw = r.get("perc_iv_en", "").strip()
        sim_raw = r.get("sim_pnl", "").strip().replace(",", "").replace("+", "")
        ivs_raw = r.get("iv_enter_sample", "").strip().rstrip("%")
        tk = r.get("ticker", "").strip()
        if not pctl_raw or pctl_raw == "n/a":
            continue
        try:
            pctl = float(pctl_raw)
            sim = float(sim_raw) if sim_raw else 0.0
            ivs = float(ivs_raw) if ivs_raw else None
        except Exception:
            continue
        rows.append((pctl, sim, tk, ivs))
        if ivs is not None:
            by_ticker.setdefault(tk, []).append((pctl, ivs))

print(f"Total trades with valid perc_iv_en + sim_pnl: {len(rows)}")
print()


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - 1)
    sx = statistics.stdev(xs)
    sy = statistics.stdev(ys)
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)


# ── 1. Bucket analysis: IV Percentile vs sim_pnl ──────────────────────────
print("=" * 70)
print("1. IV PERCENTILE BUCKETS vs sim_pnl")
print("=" * 70)

bucket_defs = [
    ("Pctl < 20",  lambda x: x < 20),
    ("Pctl 20-39", lambda x: 20 <= x < 40),
    ("Pctl 40-59", lambda x: 40 <= x < 60),
    ("Pctl 60-79", lambda x: 60 <= x < 80),
    ("Pctl 80+",   lambda x: x >= 80),
]

buckets = {name: [] for name, _ in bucket_defs}
for pctl, sim, tk, ivs in rows:
    for name, fn in bucket_defs:
        if fn(pctl):
            buckets[name].append((pctl, sim))
            break

hdr = (
    f"  {'Bucket':<14} {'Trades':>6} {'Win%':>6} {'Total PnL':>14} "
    f"{'Avg PnL':>10} {'Med PnL':>10} {'Avg Pctl':>9} "
    f"{'Min PnL':>10} {'Max PnL':>10} {'StdDev':>10}"
)
print(hdr)
print("  " + "-" * (len(hdr) - 2))

for name, _ in bucket_defs:
    data = buckets[name]
    if not data:
        print(f"  {name:<14} {'(none)':>6}")
        continue
    pnls = [d[1] for d in data]
    pctls = [d[0] for d in data]
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    avg = total / n
    med = statistics.median(pnls)
    mn = min(pnls)
    mx = max(pnls)
    sd = statistics.stdev(pnls) if n > 1 else 0
    avg_p = sum(pctls) / n
    print(
        f"  {name:<14} {n:>6} {wins/n*100:>5.1f}% {total:>+14,.2f} "
        f"{avg:>+10,.2f} {med:>+10,.2f} {avg_p:>8.1f} "
        f"{mn:>+10,.2f} {mx:>+10,.2f} {sd:>10,.2f}"
    )

print()
print("  Win/Loss breakdown:")
hdr2 = f"  {'Bucket':<14} {'Avg Win':>10} {'Avg Loss':>10} {'W/L Ratio':>10}"
print(hdr2)
print("  " + "-" * (len(hdr2) - 2))
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
    print(f"  {name:<14} {avg_w:>+10,.2f} {avg_l:>+10,.2f} {ratio:>10.2f}")

# Overall Pearson
all_pctls = [r[0] for r in rows]
all_sims = [r[1] for r in rows]
r_overall = pearson(all_pctls, all_sims)
print()
print(f"  Pearson correlation (perc_iv_en vs sim_pnl): r = {r_overall:.4f}")
print()

# ── 2. Per-ticker sanity check: IV percentile vs iv_enter_sample ──────────
print("=" * 70)
print("2. SANITY CHECK: IV Percentile vs iv_enter_sample (per-ticker)")
print("=" * 70)
print("   (Higher percentile should correspond to higher IV sample)")
print()

hdr3 = f"  {'Ticker':<8} {'Trades':>6} {'r(Pctl,IVs)':>12} {'Avg Pctl':>9} {'Avg IVs':>8} {'Min IVs':>8} {'Max IVs':>8} {'Status':>10}"
print(hdr3)
print("  " + "-" * (len(hdr3) - 2))

results = []
for tk in sorted(by_ticker):
    pairs = by_ticker[tk]
    n = len(pairs)
    pctls = [p[0] for p in pairs]
    ivss = [p[1] for p in pairs]
    r = pearson(pctls, ivss)
    avg_p = sum(pctls) / n
    avg_iv = sum(ivss) / n
    mn_iv = min(ivss)
    mx_iv = max(ivss)
    if r is None:
        direction = "FEW"
    elif r > 0.3:
        direction = "OK"
    elif r > 0:
        direction = "WEAK"
    else:
        direction = "INVERTED"
    r_str = f"{r:.4f}" if r is not None else "n/a"
    results.append((tk, n, r, direction))
    print(
        f"  {tk:<8} {n:>6} {r_str:>12} {avg_p:>8.1f} "
        f"{avg_iv:>6.1f}% {mn_iv:>6.1f}% {mx_iv:>6.1f}% {direction:>10}"
    )

ok = sum(1 for r in results if r[3] == "OK")
weak = sum(1 for r in results if r[3] == "WEAK")
inv = sum(1 for r in results if r[3] == "INVERTED")
few = sum(1 for r in results if r[3] == "FEW")
print()
print(f"  Summary: {ok} OK (r>0.3), {weak} WEAK (0<r<=0.3), {inv} INVERTED (r<=0), {few} too-few-trades")

# ── 3. Spot-check: ticker with most trades ────────────────────────────────
print()
print("=" * 70)
print("3. SPOT-CHECK: ticker with most trades - Pctl vs iv_enter_sample")
print("=" * 70)
biggest = max(by_ticker, key=lambda t: len(by_ticker[t]))
pairs = sorted(by_ticker[biggest], key=lambda p: p[0])
print(f"  Ticker: {biggest} ({len(pairs)} trades)")
print(f"  {'Pctl':>6}  {'iv_enter_sample':>16}")
for pctl, ivs in pairs:
    print(f"  {pctl:>6.0f}  {ivs:>14.1f}%")
