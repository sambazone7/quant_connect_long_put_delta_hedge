import csv, statistics

by_ticker = {}
with open("logs-qqq/parsed-march28-15days.csv") as f:
    for r in csv.DictReader(f):
        tk = r.get("ticker", "").strip()
        ivr_raw = r.get("ivr", "").strip()
        ivs_raw = r.get("iv_enter_sample", "").strip().rstrip("%")
        if not tk or not ivr_raw or ivr_raw == "n/a" or not ivs_raw or ivs_raw == "n/a":
            continue
        try:
            ivr = float(ivr_raw)
            ivs = float(ivs_raw)
        except Exception:
            continue
        by_ticker.setdefault(tk, []).append((ivr, ivs))

all_pairs = []
for pairs in by_ticker.values():
    all_pairs.extend(pairs)


def pearson(pairs):
    n = len(pairs)
    if n < 3:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - 1)
    sx = statistics.stdev(xs)
    sy = statistics.stdev(ys)
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)


print(f"Overall: {len(all_pairs)} trades across {len(by_ticker)} tickers")
r_all = pearson(all_pairs)
print(f"Overall Pearson(IVR, iv_enter_sample): r = {r_all:.4f}")
print()

header = (
    f"{'Ticker':<8} {'Trades':>6} {'r(IVR,IVs)':>11} "
    f"{'Avg IVR':>8} {'Avg IVs':>8} {'Min IVs':>8} {'Max IVs':>8} {'Status':>10}"
)
print(header)
print("-" * len(header))

results = []
for tk in sorted(by_ticker):
    pairs = by_ticker[tk]
    n = len(pairs)
    r = pearson(pairs)
    ivrs = [p[0] for p in pairs]
    ivss = [p[1] for p in pairs]
    avg_ivr = sum(ivrs) / n
    avg_ivs = sum(ivss) / n
    mn_ivs = min(ivss)
    mx_ivs = max(ivss)
    if r is None:
        direction = "FEW"
    elif r > 0.3:
        direction = "OK"
    elif r > 0:
        direction = "WEAK"
    else:
        direction = "INVERTED"
    r_str = f"{r:.4f}" if r is not None else "n/a"
    results.append((tk, n, r, avg_ivr, avg_ivs, mn_ivs, mx_ivs, direction))
    print(
        f"{tk:<8} {n:>6} {r_str:>11} {avg_ivr:>7.1f} "
        f"{avg_ivs:>6.1f}% {mn_ivs:>6.1f}% {mx_ivs:>6.1f}% {direction:>10}"
    )

ok = sum(1 for r in results if r[7] == "OK")
weak = sum(1 for r in results if r[7] == "WEAK")
inv = sum(1 for r in results if r[7] == "INVERTED")
few = sum(1 for r in results if r[7] == "FEW")
print()
print(f"Summary: {ok} OK (r>0.3), {weak} WEAK (0<r<=0.3), {inv} INVERTED (r<=0), {few} too-few-trades")

# Spot-check: ticker with most trades
print()
print("=== Spot-check: ticker with most trades — IVR vs iv_enter_sample ===")
biggest = max(by_ticker, key=lambda t: len(by_ticker[t]))
pairs = sorted(by_ticker[biggest], key=lambda p: p[0])
print(f"Ticker: {biggest} ({len(pairs)} trades)")
print(f"{'IVR':>5}  {'iv_enter_sample':>16}")
for ivr, ivs in pairs:
    print(f"{ivr:>5.0f}  {ivs:>14.1f}%")
