#!/usr/bin/env python3
"""
Analyse calendar-spread trades CSV (output of parse_log.py).

Focuses on: IV entry, ShIV/RV, and IVspread effects on profitability.

Usage:
    python analyze_cal_trades.py trades.csv                → cal-stats.out
    python analyze_cal_trades.py trades.csv -o myfile.out  → custom output
"""
import csv, sys, statistics, argparse

parser = argparse.ArgumentParser(description="Analyse calendar-spread trades CSV")
parser.add_argument("csvfile", help="Input trades CSV (from parse_log.py)")
parser.add_argument("-o", "--output", default=None,
                    help="Output file path (default: cal-stats.out)")
args = parser.parse_args()

out_path = args.output or "cal-stats.out"

# ── Helpers: parse CSV values ────────────────────────────────────────────────

def pct(s):
    """'28.1%' → 0.281, '' → None"""
    if not s or s.strip() == "":
        return None
    return float(s.replace("%", "").replace("+", "")) / 100.0

def money(s):
    """'+27301.00' → 27301.0, '' → 0.0"""
    if not s or s.strip() == "":
        return 0.0
    return float(s.replace(",", "").replace("+", ""))

def parse_signed(s):
    """'+10.6' → 10.6, '-5.1' → -5.1, '' → None"""
    if not s or s.strip() == "":
        return None
    return float(s.replace("+", ""))

def parse_float(s):
    """'1.10' → 1.10, '' → None"""
    if not s or s.strip() == "":
        return None
    return float(s)

# ── Read CSV ─────────────────────────────────────────────────────────────────

rows = []

with open(args.csvfile, "r", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Skip non-calendar rows (no long_pnl means single-put format)
        if not row.get("long_pnl", "").strip():
            continue

        combined   = money(row["combined"])
        win        = row["win"].strip() == "Win"
        long_pnl   = money(row.get("long_pnl", ""))
        short_pnl  = money(row.get("short_pnl", ""))
        stk_pnl    = money(row.get("stock_pnl", ""))
        stk_chg    = parse_signed(row.get("stk_chg_pct", ""))
        iv_entry   = pct(row.get("iv_entry", ""))
        iv_exit    = pct(row.get("iv_exit", ""))
        iv_rv      = parse_float(row.get("iv_rv", ""))
        ivspread   = pct(row.get("ivspread", ""))
        shiv_rv    = parse_float(row.get("shiv_rv", ""))

        rows.append({
            "ticker":      row["ticker"],
            "earnings":    row["earnings"],
            "combined":    combined,
            "long_pnl":    long_pnl,
            "short_pnl":   short_pnl,
            "stk_pnl":     stk_pnl,
            "stk_chg_pct": stk_chg,
            "iv_entry":    iv_entry,
            "iv_exit":     iv_exit,
            "iv_rv":       iv_rv,
            "ivspread":    ivspread,
            "shiv_rv":     shiv_rv,
            "win":         win,
        })

out = open(out_path, "w", encoding="utf-8")
out.write(f"Total calendar trades: {len(rows)}\n\n")

# ── Reporting helpers ────────────────────────────────────────────────────────

def write_bucket(label, bucket):
    """Detailed multi-line summary for a bucket."""
    n = len(bucket)
    if n == 0:
        out.write(f"{label}:\n  (no trades)\n\n")
        return
    total   = sum(r["combined"] for r in bucket)
    avg     = total / n
    med     = statistics.median([r["combined"] for r in bucket])
    wins    = sum(r["win"] for r in bucket)
    avg_l   = sum(r["long_pnl"] for r in bucket) / n
    avg_s   = sum(r["short_pnl"] for r in bucket) / n
    avg_stk = sum(r["stk_pnl"] for r in bucket) / n
    out.write(f"{label}:\n")
    out.write(f"  Trades:        {n}\n")
    out.write(f"  Total PnL:     ${total:>+14,.2f}\n")
    out.write(f"  Avg PnL:       ${avg:>+14,.2f}\n")
    out.write(f"  Median PnL:    ${med:>+14,.2f}\n")
    out.write(f"  Win rate:      {wins/n:.1%}  ({wins}/{n})\n")
    out.write(f"  Avg Long PnL:  ${avg_l:>+14,.2f}\n")
    out.write(f"  Avg Short PnL: ${avg_s:>+14,.2f}\n")
    out.write(f"  Avg Stk PnL:   ${avg_stk:>+14,.2f}\n")
    out.write("\n")

def write_band_row(label, bucket):
    """Single compact row for a band table."""
    n = len(bucket)
    if n == 0:
        out.write(f"  {label:<25} {n:>4}    ---\n")
        return
    total = sum(r["combined"] for r in bucket)
    avg   = total / n
    med   = statistics.median([r["combined"] for r in bucket])
    wins  = sum(r["win"] for r in bucket)
    out.write(f"  {label:<25} {n:>4} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/n:.1%}\n")

def write_quintile(label, sorted_rows):
    """Quintile table for a sorted list of rows."""
    n = len(sorted_rows)
    if n < 5:
        out.write(f"  (fewer than 5 trades — skipping quintile analysis)\n\n")
        return
    q_size = n // 5
    for qi in range(5):
        start = qi * q_size
        end   = (qi + 1) * q_size if qi < 4 else n
        qb    = sorted_rows[start:end]
        nq    = len(qb)
        if nq == 0:
            continue
        lo_v  = qb[0][label]
        hi_v  = qb[-1][label]
        total = sum(r["combined"] for r in qb)
        avg   = total / nq
        med   = statistics.median([r["combined"] for r in qb])
        wins  = sum(r["win"] for r in qb)
        out.write(f"  Q{qi+1}      {lo_v:>8.1%} -  {hi_v:>7.1%}   {nq:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/nq:.1%}\n")
    out.write("\n")

def write_quintile_float(label, sorted_rows):
    """Quintile table for a sorted list of rows (float values, not percentages)."""
    n = len(sorted_rows)
    if n < 5:
        out.write(f"  (fewer than 5 trades — skipping quintile analysis)\n\n")
        return
    q_size = n // 5
    for qi in range(5):
        start = qi * q_size
        end   = (qi + 1) * q_size if qi < 4 else n
        qb    = sorted_rows[start:end]
        nq    = len(qb)
        if nq == 0:
            continue
        lo_v  = qb[0][label]
        hi_v  = qb[-1][label]
        total = sum(r["combined"] for r in qb)
        avg   = total / nq
        med   = statistics.median([r["combined"] for r in qb])
        wins  = sum(r["win"] for r in qb)
        out.write(f"  Q{qi+1}      {lo_v:>8.2f} -  {hi_v:>7.2f}   {nq:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/nq:.1%}\n")
    out.write("\n")


# ════════════════════════════════════════════════════════════════════════════
# 1. OVERALL SUMMARY
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("1. OVERALL SUMMARY\n")
out.write("=" * 80 + "\n\n")
write_bucket("All calendar trades", rows)

# ════════════════════════════════════════════════════════════════════════════
# 2. EFFECT OF IV ENTRY ON PROFIT/LOSS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("2. EFFECT OF IV ENTRY ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

iv_rows = [r for r in rows if r["iv_entry"] is not None]

if len(iv_rows) >= 5:
    out.write("IV entry quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'IV Range':<22}     {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("-" * 95 + "\n")
    sorted_iv = sorted(iv_rows, key=lambda r: r["iv_entry"])
    write_quintile("iv_entry", sorted_iv)

out.write("IV entry fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

iv_bands = [
    ("IV < 20%",            lambda r: r["iv_entry"] is not None and r["iv_entry"] < 0.20),
    ("20% <= IV < 30%",     lambda r: r["iv_entry"] is not None and 0.20 <= r["iv_entry"] < 0.30),
    ("30% <= IV < 40%",     lambda r: r["iv_entry"] is not None and 0.30 <= r["iv_entry"] < 0.40),
    ("40% <= IV < 50%",     lambda r: r["iv_entry"] is not None and 0.40 <= r["iv_entry"] < 0.50),
    ("IV >= 50%",           lambda r: r["iv_entry"] is not None and r["iv_entry"] >= 0.50),
]
for label, filt in iv_bands:
    b = [r for r in rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with IV entry)", iv_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 3. EFFECT OF ShIV/RV ON PROFIT/LOSS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("3. EFFECT OF ShIV/RV (Short Put IV / Realized Vol) ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

shiv_rows = [r for r in rows if r["shiv_rv"] is not None]

if len(shiv_rows) >= 5:
    out.write("ShIV/RV quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'ShIV/RV Range':<22}  {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("-" * 95 + "\n")
    sorted_shiv = sorted(shiv_rows, key=lambda r: r["shiv_rv"])
    write_quintile_float("shiv_rv", sorted_shiv)

out.write("ShIV/RV fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

shiv_bands = [
    ("ShIV/RV < 0.80",         lambda r: r["shiv_rv"] is not None and r["shiv_rv"] < 0.80),
    ("0.80 <= ShIV/RV < 1.00", lambda r: r["shiv_rv"] is not None and 0.80 <= r["shiv_rv"] < 1.00),
    ("1.00 <= ShIV/RV < 1.20", lambda r: r["shiv_rv"] is not None and 1.00 <= r["shiv_rv"] < 1.20),
    ("1.20 <= ShIV/RV < 1.50", lambda r: r["shiv_rv"] is not None and 1.20 <= r["shiv_rv"] < 1.50),
    ("ShIV/RV >= 1.50",        lambda r: r["shiv_rv"] is not None and r["shiv_rv"] >= 1.50),
]
for label, filt in shiv_bands:
    b = [r for r in shiv_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with ShIV/RV)", shiv_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 4. EFFECT OF IVspread ON PROFIT/LOSS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("4. EFFECT OF IVspread (Long Put IV - Short Put IV at entry) ON PROFIT/LOSS\n")
out.write("=" * 80 + "\n\n")

spr_rows = [r for r in rows if r["ivspread"] is not None]

if len(spr_rows) >= 5:
    out.write("IVspread quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'Spread Range':<22}    {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("-" * 95 + "\n")
    sorted_spr = sorted(spr_rows, key=lambda r: r["ivspread"])
    write_quintile("ivspread", sorted_spr)

out.write("IVspread fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

spr_bands = [
    ("IVspread < 2%",          lambda r: r["ivspread"] is not None and r["ivspread"] < 0.02),
    ("2% <= IVspread < 5%",    lambda r: r["ivspread"] is not None and 0.02 <= r["ivspread"] < 0.05),
    ("5% <= IVspread < 10%",   lambda r: r["ivspread"] is not None and 0.05 <= r["ivspread"] < 0.10),
    ("10% <= IVspread < 15%",  lambda r: r["ivspread"] is not None and 0.10 <= r["ivspread"] < 0.15),
    ("IVspread >= 15%",        lambda r: r["ivspread"] is not None and r["ivspread"] >= 0.15),
]
for label, filt in spr_bands:
    b = [r for r in spr_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with IVspread)", spr_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 5. COMBINED: ShIV/RV + IV ENTRY BANDS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("5. COMBINED: ShIV/RV FILTER + IV ENTRY BANDS\n")
out.write("=" * 80 + "\n\n")

both_rows = [r for r in rows if r["shiv_rv"] is not None and r["iv_entry"] is not None]

for shiv_label, shiv_filt in [
    ("ShIV/RV < 1.00", lambda r: r["shiv_rv"] < 1.00),
    ("ShIV/RV >= 1.00", lambda r: r["shiv_rv"] >= 1.00),
]:
    subset = [r for r in both_rows if shiv_filt(r)]
    out.write(f"{shiv_label}  ({len(subset)} trades)\n")
    out.write(f"{'  IV Band':<27} {'N':>5} {'Total PnL':>16} {'Avg PnL':>12} {'Median PnL':>14}    {'Win%'}\n")
    out.write("  " + "-" * 83 + "\n")
    for iv_label, iv_filt in iv_bands:
        b = [r for r in subset if iv_filt(r)]
        write_band_row(iv_label, b)
    out.write("  " + "-" * 83 + "\n")
    n = len(subset)
    if n > 0:
        total = sum(r["combined"] for r in subset)
        avg   = total / n
        med   = statistics.median([r["combined"] for r in subset])
        wins  = sum(r["win"] for r in subset)
        out.write(f"  {'TOTAL':<25} {n:>4} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/n:.1%}\n")
    out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 6. TOP 5 WINNERS & LOSERS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("6. TOP 5 WINNERS & LOSERS BY ShIV/RV BUCKET\n")
out.write("=" * 80 + "\n\n")

lo_shiv = [r for r in shiv_rows if r["shiv_rv"] < 1.00]
hi_shiv = [r for r in shiv_rows if r["shiv_rv"] >= 1.00]

for bucket_label, bucket in [("ShIV/RV < 1.00", lo_shiv), ("ShIV/RV >= 1.00", hi_shiv)]:
    out.write(f"{bucket_label}  ({len(bucket)} trades):\n")
    sorted_b = sorted(bucket, key=lambda r: r["combined"])
    out.write("  Top 5 losers:\n")
    for r in sorted_b[:5]:
        iv_s   = f"IV={r['iv_entry']:.1%}" if r["iv_entry"] is not None else "IV=n/a"
        shiv_s = f"ShIV/RV={r['shiv_rv']:.2f}" if r["shiv_rv"] is not None else "ShIV/RV=n/a"
        spr_s  = f"IVspr={r['ivspread']:.1%}" if r["ivspread"] is not None else "IVspr=n/a"
        out.write(f"    {r['ticker']:<6} {r['earnings']}  {iv_s}  {shiv_s}  {spr_s}  PnL=${r['combined']:>+12,.2f}\n")
    out.write("  Top 5 winners:\n")
    for r in sorted_b[-5:]:
        iv_s   = f"IV={r['iv_entry']:.1%}" if r["iv_entry"] is not None else "IV=n/a"
        shiv_s = f"ShIV/RV={r['shiv_rv']:.2f}" if r["shiv_rv"] is not None else "ShIV/RV=n/a"
        spr_s  = f"IVspr={r['ivspread']:.1%}" if r["ivspread"] is not None else "IVspr=n/a"
        out.write(f"    {r['ticker']:<6} {r['earnings']}  {iv_s}  {shiv_s}  {spr_s}  PnL=${r['combined']:>+12,.2f}\n")
    out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 7. EFFECT OF STOCK CHANGE % ON PROFIT/LOSS
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("7. EFFECT OF STOCK CHANGE % ON PROFIT/LOSS\n")
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
print(f"Analysis -> {out_path}  ({len(rows)} calendar trades)")
