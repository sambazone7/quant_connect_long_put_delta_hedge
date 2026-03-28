import csv, statistics

rows = []
with open("logs-qqq/parsed-march28-20days.csv") as f:
    for r in csv.DictReader(f):
        vixen = r.get("vix_entry", "").strip()
        vixxs = r.get("vix_exit", "").strip()
        iven = r.get("iv_enter_sample", "").strip().rstrip("%")
        ivex = r.get("iv_exit_sample", "").strip().rstrip("%")
        sim = r.get("sim_pnl", "").strip().replace(",", "").replace("+", "")
        if not all([vixen, vixxs, iven, ivex]):
            continue
        try:
            vixen_f = float(vixen)
            vixxs_f = float(vixxs)
            iven_f = float(iven)
            ivex_f = float(ivex)
            sim_f = float(sim) if sim else 0.0
        except Exception:
            continue
        if vixen_f <= 0 or iven_f <= 0:
            continue
        pct_dvix = (vixxs_f - vixen_f) / vixen_f * 100
        pct_div = (ivex_f - iven_f) / iven_f * 100
        rows.append((pct_dvix, pct_div, sim_f, vixen_f, vixxs_f, iven_f, ivex_f))

print(f"Total trades with valid VIX + iv_enter/exit_sample: {len(rows)}")
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


def spearman(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    rx = [0] * n
    ry = [0] * n
    for ranks, vals in [(rx, xs), (ry, ys)]:
        order = sorted(range(n), key=lambda i: vals[i])
        for rank, idx in enumerate(order):
            ranks[idx] = rank + 1
    return pearson(rx, ry)


dvix = [r[0] for r in rows]
div = [r[1] for r in rows]

print("=" * 60)
print("1. CORRELATION: %dVIX vs %dIV_sample (long-term IV)")
print("=" * 60)
r_p = pearson(dvix, div)
r_s = spearman(dvix, div)
print(f"   Pearson  r = {r_p:.4f}")
print(f"   Spearman rho = {r_s:.4f}")
print()

same_sign = sum(1 for d, i in zip(dvix, div) if (d > 0 and i > 0) or (d < 0 and i < 0))
diff_sign = sum(1 for d, i in zip(dvix, div) if (d > 0 and i < 0) or (d < 0 and i > 0))
zero_cases = len(rows) - same_sign - diff_sign
print("=" * 60)
print("2. CONCORDANCE (sign agreement)")
print("=" * 60)
print(f"   Same direction:     {same_sign:>5}  ({same_sign/len(rows)*100:.1f}%)")
print(f"   Opposite direction: {diff_sign:>5}  ({diff_sign/len(rows)*100:.1f}%)")
print(f"   One side zero:      {zero_cases:>5}  ({zero_cases/len(rows)*100:.1f}%)")
print()

quads = {
    "VIX down + IVs down": [],
    "VIX down + IVs up":   [],
    "VIX up   + IVs down": [],
    "VIX up   + IVs up":   [],
}
for pct_dvix, pct_div, sim_f, *_ in rows:
    if pct_dvix <= 0 and pct_div <= 0:
        quads["VIX down + IVs down"].append((pct_dvix, pct_div, sim_f))
    elif pct_dvix <= 0 and pct_div > 0:
        quads["VIX down + IVs up"].append((pct_dvix, pct_div, sim_f))
    elif pct_dvix > 0 and pct_div <= 0:
        quads["VIX up   + IVs down"].append((pct_dvix, pct_div, sim_f))
    else:
        quads["VIX up   + IVs up"].append((pct_dvix, pct_div, sim_f))

print("=" * 60)
print("3. QUADRANT ANALYSIS")
print("=" * 60)
hdr = f"  {'Quadrant':<24} {'Trades':>6} {'%':>5} {'Avg %dVIX':>10} {'Avg %dIVs':>10} {'Avg SimPnL':>12} {'Win%':>6}"
print(hdr)
print("  " + "-" * (len(hdr) - 2))
for name in ["VIX down + IVs down", "VIX down + IVs up", "VIX up   + IVs down", "VIX up   + IVs up"]:
    data = quads[name]
    if not data:
        print(f"  {name:<24} {'(none)':>6}")
        continue
    n = len(data)
    avg_dvix = sum(d[0] for d in data) / n
    avg_div = sum(d[1] for d in data) / n
    avg_sim = sum(d[2] for d in data) / n
    wins = sum(1 for d in data if d[2] > 0)
    print(f"  {name:<24} {n:>6} {n/len(rows)*100:>4.1f}% {avg_dvix:>+9.1f}% {avg_div:>+9.1f}% {avg_sim:>+12,.2f} {wins/n*100:>5.1f}%")

print()

print("=" * 60)
print("4. BUCKETED BY VIX % CHANGE - IV_sample response")
print("=" * 60)
vix_buckets = [
    ("VIX fell >20%",    lambda x: x <= -20),
    ("VIX fell 10-20%",  lambda x: -20 < x <= -10),
    ("VIX fell 0-10%",   lambda x: -10 < x <= 0),
    ("VIX rose 0-10%",   lambda x: 0 < x <= 10),
    ("VIX rose 10-20%",  lambda x: 10 < x <= 20),
    ("VIX rose >20%",    lambda x: x > 20),
]

hdr2 = f"  {'VIX Bucket':<20} {'Trades':>6} {'Avg %dVIX':>10} {'Avg %dIVs':>10} {'Med %dIVs':>10} {'Avg SimPnL':>12} {'Win%':>6}"
print(hdr2)
print("  " + "-" * (len(hdr2) - 2))
for name, fn in vix_buckets:
    data = [(d, i, s) for d, i, s, *_ in rows if fn(d)]
    if not data:
        print(f"  {name:<20} {'(none)':>6}")
        continue
    n = len(data)
    avg_dvix = sum(d[0] for d in data) / n
    avg_div = sum(d[1] for d in data) / n
    med_div = statistics.median([d[1] for d in data])
    avg_sim = sum(d[2] for d in data) / n
    wins = sum(1 for d in data if d[2] > 0)
    print(f"  {name:<20} {n:>6} {avg_dvix:>+9.1f}% {avg_div:>+9.1f}% {med_div:>+9.1f}% {avg_sim:>+12,.2f} {wins/n*100:>5.1f}%")

print()

print("=" * 60)
print("5. PER-TICKER CORRELATION (top 15 by trade count)")
print("=" * 60)
by_ticker = {}
with open("logs-qqq/parsed-march28-20days.csv") as f:
    for r in csv.DictReader(f):
        tk = r.get("ticker", "").strip()
        vixen = r.get("vix_entry", "").strip()
        vixxs = r.get("vix_exit", "").strip()
        iven = r.get("iv_enter_sample", "").strip().rstrip("%")
        ivex = r.get("iv_exit_sample", "").strip().rstrip("%")
        if not all([tk, vixen, vixxs, iven, ivex]):
            continue
        try:
            vixen_f = float(vixen)
            vixxs_f = float(vixxs)
            iven_f = float(iven)
            ivex_f = float(ivex)
        except Exception:
            continue
        if vixen_f <= 0 or iven_f <= 0:
            continue
        pct_dvix = (vixxs_f - vixen_f) / vixen_f * 100
        pct_div = (ivex_f - iven_f) / iven_f * 100
        by_ticker.setdefault(tk, []).append((pct_dvix, pct_div))

top = sorted(by_ticker.items(), key=lambda x: -len(x[1]))[:15]
hdr3 = f"  {'Ticker':<8} {'Trades':>6} {'Pearson':>8} {'Spearman':>9} {'Concord%':>9}"
print(hdr3)
print("  " + "-" * (len(hdr3) - 2))
for tk, pairs in top:
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    rp = pearson(xs, ys)
    rs = spearman(xs, ys)
    conc = sum(1 for x, y in pairs if (x > 0 and y > 0) or (x < 0 and y < 0))
    rp_s = f"{rp:.3f}" if rp is not None else "n/a"
    rs_s = f"{rs:.3f}" if rs is not None else "n/a"
    print(f"  {tk:<8} {len(pairs):>6} {rp_s:>8} {rs_s:>9} {conc/len(pairs)*100:>8.1f}%")
