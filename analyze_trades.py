#!/usr/bin/env python3
"""
Analyse trades.csv -- IV/RV filter, Entry IV, IV min/max, MaxD effect on PnL.

Usage:
      python analyze_trades.py trades.csv                        → stats.out
      python analyze_trades.py trades.csv --v2                   → v2-stats.out (outlier-filtered)
      python analyze_trades.py trades.csv -o myfile.out          → custom output
"""
import csv, sys, statistics, argparse

parser = argparse.ArgumentParser()
parser.add_argument("csvfile", help="Input trades CSV (output of parse_log.py)")
parser.add_argument("-o", "--output", default=None,
                    help="Output file path (default: stats.out or v2-stats.out with --v2)")
parser.add_argument("--v2", action="store_true",
                    help="Exclude combined loss > $6k and combined profit > $10k")
args = parser.parse_args()

out_path = args.output or ("v2-stats.out" if args.v2 else "stats.out")

# ── Read CSV ────────────────────────────────────────────────────────────────

def pct(s):
    """'28.1%' → 0.281, '' → None"""
    if not s or s.strip() == "":
        return None
    return float(s.replace("%", "").replace("+", "")) / 100.0

def money(s):
    """'+27301.00' → 27301.0"""
    if not s or s.strip() == "":
        return 0.0
    return float(s.replace(",", "").replace("+", ""))

def parse_signed(s):
    """'+10.6' → 10.6, '-5.1' → -5.1, '' → None"""
    if not s or s.strip() == "":
        return None
    return float(s.replace("+", ""))

def parse_int(s):
    """'27' → 27, '' → None"""
    if not s or s.strip() == "":
        return None
    return int(s)

all_rows = []

with open(args.csvfile, "r", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    for row in reader:
        combined  = money(row["combined"])
        win       = row["win"].strip() == "Win"
        iv_entry  = pct(row.get("iv_entry", ""))
        iv_exit   = pct(row.get("iv_exit", ""))
        iv_rv     = float(row["iv_rv"]) if row.get("iv_rv", "").strip() else None
        iv_min    = pct(row.get("iv_min", ""))
        iv_max    = pct(row.get("iv_max", ""))
        mind      = parse_int(row.get("iv_min_days_before", ""))
        maxd      = parse_int(row.get("iv_max_days_before", ""))
        put_pnl   = money(row.get("put_pnl", ""))
        stk_pnl   = money(row.get("stock_pnl", ""))
        stk_chg   = parse_signed(row.get("stk_chg_pct", ""))

        all_rows.append({
            "ticker":      row["ticker"],
            "earnings":    row["earnings"],
            "combined":    combined,
            "put_pnl":     put_pnl,
            "stk_pnl":     stk_pnl,
            "stk_chg_pct": stk_chg,
            "iv_entry":    iv_entry,
            "iv_exit":     iv_exit,
            "iv_rv":       iv_rv,
            "win":         win,
            "iv_min":      iv_min,
            "iv_max":      iv_max,
            "mind":        mind,
            "maxd":        maxd,
        })

# ── Outlier filter (--v2) ───────────────────────────────────────────────────

out = open(out_path, "w", encoding="utf-8")

if args.v2:
    rows = [r for r in all_rows if -6000 <= r["combined"] <= 10000]
    excluded_count = len(all_rows) - len(rows)
    out.write(f"Total trades (before filter): {len(all_rows)}\n")
    out.write(f"Excluded outliers (loss > $6k or profit > $10k): {excluded_count}\n")
    out.write(f"Trades after filter: {len(rows)}\n\n")
else:
    rows = all_rows
    out.write(f"Total trades: {len(rows)}\n\n")

# ── Helpers ─────────────────────────────────────────────────────────────────

def write_bucket(label, bucket):
    n = len(bucket)
    if n == 0:
        out.write(f"{label}:\n  (no trades)\n\n")
        return
    total = sum(r["combined"] for r in bucket)
    avg   = total / n
    med   = statistics.median([r["combined"] for r in bucket])
    wins  = sum(r["win"] for r in bucket)
    avg_p = sum(r["put_pnl"] for r in bucket) / n
    avg_s = sum(r["stk_pnl"] for r in bucket) / n
    out.write(f"{label}:\n")
    out.write(f"  Trades:      {n}\n")
    out.write(f"  Total PnL:   ${total:>+14,.2f}\n")
    out.write(f"  Avg PnL:     ${avg:>+14,.2f}\n")
    out.write(f"  Median PnL:  ${med:>+14,.2f}\n")
    out.write(f"  Win rate:    {wins/n:.1%}  ({wins}/{n})\n")
    out.write(f"  Avg Put PnL: ${avg_p:>+14,.2f}\n")
    out.write(f"  Avg Stk PnL: ${avg_s:>+14,.2f}\n")
    out.write("\n")

def write_band_row(label, bucket):
    n = len(bucket)
    if n == 0:
        out.write(f"  {label:<25} {n:>4}    ---\n")
        return
    total = sum(r["combined"] for r in bucket)
    avg   = total / n
    med   = statistics.median([r["combined"] for r in bucket])
    wins  = sum(r["win"] for r in bucket)
    out.write(f"  {label:<25} {n:>4} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/n:.1%}\n")

# ── 1. IV/RV Filter Analysis ───────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("1. EFFECT OF IV/RV FILTER ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

rv_rows = [r for r in rows if r["iv_rv"] is not None]

b1 = [r for r in rv_rows if r["iv_rv"] < 1.0]
b2 = [r for r in rv_rows if 1.0 <= r["iv_rv"] < 1.5]
b3 = [r for r in rv_rows if r["iv_rv"] >= 1.5]
b12 = [r for r in rv_rows if r["iv_rv"] < 1.5]

write_bucket("IV/RV < 1.0", b1)
write_bucket("1.0 <= IV/RV < 1.5", b2)
write_bucket("IV/RV >= 1.5", b3)
write_bucket("IV/RV < 1.5 (combined)", b12)
write_bucket("ALL trades (with IV/RV)", rv_rows)

# ── 2. Entry IV Effect ─────────────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("2. EFFECT OF ENTRY IV ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

iv_rows = [r for r in rows if r["iv_entry"] is not None]
iv_vals = sorted([r["iv_entry"] for r in iv_rows])

if len(iv_vals) >= 5:
    out.write("Entry IV quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'IV Range':<22}     {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("-" * 95 + "\n")
    sorted_rows = sorted(iv_rows, key=lambda r: r["iv_entry"])
    q_size = len(sorted_rows) // 5
    for qi in range(5):
        start = qi * q_size
        end = (qi + 1) * q_size if qi < 4 else len(sorted_rows)
        qb = sorted_rows[start:end]
        n = len(qb)
        if n == 0:
            continue
        lo_v = qb[0]["iv_entry"]
        hi_v = qb[-1]["iv_entry"]
        total = sum(r["combined"] for r in qb)
        avg = total / n
        med = statistics.median([r["combined"] for r in qb])
        wins = sum(r["win"] for r in qb)
        out.write(f"  Q{qi+1}      {lo_v:>8.1%} -  {hi_v:>7.1%}   {n:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/n:.1%}\n")
    out.write("\n")

out.write("Entry IV fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

bands = [
    ("IV < 20%",            lambda r: r["iv_entry"] is not None and r["iv_entry"] < 0.20),
    ("20% <= IV < 25%",     lambda r: r["iv_entry"] is not None and 0.20 <= r["iv_entry"] < 0.25),
    ("25% <= IV < 30%",     lambda r: r["iv_entry"] is not None and 0.25 <= r["iv_entry"] < 0.30),
    ("30% <= IV < 40%",     lambda r: r["iv_entry"] is not None and 0.30 <= r["iv_entry"] < 0.40),
    ("40% <= IV < 50%",     lambda r: r["iv_entry"] is not None and 0.40 <= r["iv_entry"] < 0.50),
    ("IV >= 50%",           lambda r: r["iv_entry"] is not None and r["iv_entry"] >= 0.50),
]
for label, filt in bands:
    b = [r for r in rows if filt(r)]
    write_band_row(label, b)
out.write("\n")

# ── 3. Combined: IV/RV < 1.5 + Entry IV ────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("3. COMBINED: IV/RV < 1.5 FILTER + ENTRY IV BANDS\n")
out.write("=" * 80 + "\n\n")

filtered = [r for r in rows if r["iv_rv"] is not None and r["iv_rv"] < 1.5]

out.write(f"{'Band':<25} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
out.write("-" * 85 + "\n")
for label, filt in bands:
    b = [r for r in filtered if r["iv_entry"] is not None and filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")

# Summary totals
n = len(filtered)
if n > 0:
    total_pnl = sum(r["combined"] for r in filtered)
    avg_pnl = total_pnl / n
    med_pnl = statistics.median([r["combined"] for r in filtered])
    wins = sum(r["win"] for r in filtered)
    out.write(f"  {'TOTAL IV/RV<1.5':<25} {n:>4} ${total_pnl:>+14,.2f} ${avg_pnl:>+12,.2f} ${med_pnl:>+14,.2f}   {wins/n:.1%}\n")

excluded = [r for r in rows if r["iv_rv"] is not None and r["iv_rv"] >= 1.5]
n2 = len(excluded)
if n2 > 0:
    total_pnl2 = sum(r["combined"] for r in excluded)
    avg_pnl2 = total_pnl2 / n2
    med_pnl2 = statistics.median([r["combined"] for r in excluded])
    wins2 = sum(r["win"] for r in excluded)
    out.write(f"  {'TOTAL IV/RV>=1.5':<25} {n2:>4} ${total_pnl2:>+14,.2f} ${avg_pnl2:>+12,.2f} ${med_pnl2:>+14,.2f}   {wins2/n2:.1%}\n")
out.write("\n")

# ── 4. Top 5 Winners & Losers ──────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("4. TOP 5 WINNERS & LOSERS BY IV/RV BUCKET\n")
out.write("=" * 80 + "\n\n")

for bucket_label, bucket in [("IV/RV < 1.5", filtered), ("IV/RV >= 1.5", excluded)]:
    out.write(f"{bucket_label}:\n")
    sorted_b = sorted(bucket, key=lambda r: r["combined"])
    out.write("  Top 5 losers:\n")
    for r in sorted_b[:5]:
        iv_s = f"IV= {r['iv_entry']:.1%}" if r["iv_entry"] is not None else "IV=n/a"
        rv_s = f"IV/RV={r['iv_rv']:.2f}" if r["iv_rv"] is not None else "IV/RV=n/a"
        out.write(f"    {r['ticker']:<6} {r['earnings']}  {iv_s}  {rv_s}  PnL=${r['combined']:>+12,.2f}\n")
    out.write("  Top 5 winners:\n")
    for r in sorted_b[-5:]:
        iv_s = f"IV= {r['iv_entry']:.1%}" if r["iv_entry"] is not None else "IV=n/a"
        rv_s = f"IV/RV={r['iv_rv']:.2f}" if r["iv_rv"] is not None else "IV/RV=n/a"
        out.write(f"    {r['ticker']:<6} {r['earnings']}  {iv_s}  {rv_s}  PnL=${r['combined']:>+12,.2f}\n")
    out.write("\n")

# ── 5. IV Spread (iv_max - iv_min) ─────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("5. EFFECT OF IV SPREAD (IV max - IV min) ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

spread_rows = [r for r in rows if r["iv_min"] is not None and r["iv_max"] is not None]
for r in spread_rows:
    r["_iv_spread"] = r["iv_max"] - r["iv_min"]

if spread_rows:
    spreads = sorted([r["_iv_spread"] for r in spread_rows])
    out.write("IV spread quartile analysis:\n")
    out.write(f"{'Quartile':<12} {'Spread Range':<24} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("-" * 100 + "\n")
    sorted_sr = sorted(spread_rows, key=lambda r: r["_iv_spread"])
    q_size = len(sorted_sr) // 4
    for qi in range(4):
        start = qi * q_size
        end = (qi + 1) * q_size if qi < 3 else len(sorted_sr)
        qb = sorted_sr[start:end]
        n = len(qb)
        if n == 0:
            continue
        lo_v = qb[0]["_iv_spread"]
        hi_v = qb[-1]["_iv_spread"]
        total = sum(r["combined"] for r in qb)
        avg = total / n
        med = statistics.median([r["combined"] for r in qb])
        wins = sum(r["win"] for r in qb)
        out.write(f"  Q{qi+1}       {lo_v:>8.1%} - {hi_v:>8.1%}     {n:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+14,.2f}   {wins/n:.1%}\n")
    out.write("\n")

# ── 6. MinD Effect ──────────────────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("6. EFFECT OF MinD (DAYS BEFORE EARNINGS WHEN IV WAS LOWEST)\n")
out.write("=" * 80 + "\n\n")

mind_rows = [r for r in rows if r["mind"] is not None]

out.write(f"{'Band':<33} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
out.write("-" * 90 + "\n")

mind_bands = [
    ("MinD = 0 (min on exit)",   lambda r: r["mind"] == 0),
    ("MinD = 1",                 lambda r: r["mind"] == 1),
    ("MinD = 2",                 lambda r: r["mind"] == 2),
    ("MinD 3-5",                 lambda r: 3 <= r["mind"] <= 5),
    ("MinD 6-10",                lambda r: 6 <= r["mind"] <= 10),
    ("MinD > 10",                lambda r: r["mind"] is not None and r["mind"] > 10),
]
for label, filt in mind_bands:
    b = [r for r in mind_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 90 + "\n")
write_band_row("ALL (with MinD)", mind_rows)
out.write("\n")

# ── 7. MaxD Effect ──────────────────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("7. EFFECT OF MaxD (DAYS BEFORE EARNINGS WHEN IV PEAKED)\n")
out.write("=" * 80 + "\n\n")

maxd_rows = [r for r in rows if r["maxd"] is not None]

out.write(f"{'Band':<33} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
out.write("-" * 90 + "\n")

maxd_bands = [
    ("MaxD = 0 (peak on exit)",  lambda r: r["maxd"] == 0),
    ("MaxD = 1",                 lambda r: r["maxd"] == 1),
    ("MaxD = 2",                 lambda r: r["maxd"] == 2),
    ("MaxD 3-5",                 lambda r: 3 <= r["maxd"] <= 5),
    ("MaxD 6-10",                lambda r: 6 <= r["maxd"] <= 10),
    ("MaxD > 10",                lambda r: r["maxd"] is not None and r["maxd"] > 10),
]
for label, filt in maxd_bands:
    b = [r for r in maxd_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 90 + "\n")
write_band_row("ALL (with MaxD)", maxd_rows)
out.write("\n")

# ── 8. Stock Change % Effect ───────────────────────────────────────────────

out.write("=" * 80 + "\n")
out.write("8. EFFECT OF STOCK CHANGE % ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

stk_rows = [r for r in rows if r["stk_chg_pct"] is not None]

out.write(f"{'Band':<25} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
out.write("-" * 88 + "\n")

stk_bands = [
    ("Stk < -5%",           lambda r: r["stk_chg_pct"] < -5),
    ("-5% <= Stk < -2%",    lambda r: -5 <= r["stk_chg_pct"] < -2),
    ("-2% <= Stk < 0%",     lambda r: -2 <= r["stk_chg_pct"] < 0),
    ("0% <= Stk < 2%",      lambda r: 0  <= r["stk_chg_pct"] < 2),
    ("2% <= Stk < 5%",      lambda r: 2  <= r["stk_chg_pct"] < 5),
    ("Stk >= 5%",           lambda r: r["stk_chg_pct"] >= 5),
]
for label, filt in stk_bands:
    b = [r for r in stk_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 88 + "\n")
write_band_row("ALL (with Stk Chg%)", stk_rows)
out.write("\n")

out.close()
print(f"Analysis -> {out_path}  ({len(rows)} trades)")
