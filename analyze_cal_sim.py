#!/usr/bin/env python3
"""
Analyse calendar-spread trades CSV for sim_pnl relationships.

Focuses on: VIX entry, IV long/short ratio, and |stock change %| vs sim_pnl.

Usage:
    python analyze_cal_sim.py <input.csv> <output.out>
"""
import csv, sys, statistics, argparse

parser = argparse.ArgumentParser(description="Analyse sim_pnl relationships in calendar-spread trades")
parser.add_argument("csvfile", help="Input trades CSV (from cal_parse_log.py)")
parser.add_argument("output",  help="Output file path")
args = parser.parse_args()

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

def _fmt(v, fmt_str, suffix="", na="n/a"):
    if v is None:
        return na.rjust(len(na))
    return f"{v:{fmt_str}}{suffix}"

# ── Read CSV ─────────────────────────────────────────────────────────────────

rows = []

with open(args.csvfile, "r", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    for row in reader:
        sim_pnl = money(row.get("sim_pnl", ""))
        vix_en  = parse_float(row.get("vix_entry", ""))
        vix_ex  = parse_float(row.get("vix_exit", ""))
        iv_long = pct(row.get("iv_long_entry", ""))
        iv_short = pct(row.get("iv_short_entry", ""))
        stk_chg = parse_signed(row.get("stk_chg_pct", ""))
        stk_pnl   = money(row.get("stock_pnl", ""))
        long_pnl  = money(row.get("long_pnl", ""))
        short_pnl = money(row.get("short_pnl", ""))

        iv_ratio = None
        if iv_long is not None and iv_short is not None and iv_short > 0:
            iv_ratio = iv_long / iv_short

        rows.append({
            "ticker":      row.get("ticker", ""),
            "earnings":    row.get("earnings", ""),
            "sim_pnl":     sim_pnl,
            "win":         sim_pnl >= 0,
            "vix_entry":   vix_en,
            "vix_exit":    vix_ex,
            "iv_long":     iv_long,
            "iv_short":    iv_short,
            "iv_ratio":    iv_ratio,
            "stk_chg_pct": stk_chg,
            "stk_pnl":     stk_pnl,
            "long_pnl":    long_pnl,
            "short_pnl":   short_pnl,
            "abs_stk_chg": abs(stk_chg) if stk_chg is not None else None,
        })

out = open(args.output, "w", encoding="utf-8")

# ── Reporting helpers ────────────────────────────────────────────────────────

def write_summary(label, bucket):
    n = len(bucket)
    if n == 0:
        out.write(f"{label}:\n  (no trades)\n\n")
        return
    total = sum(r["sim_pnl"] for r in bucket)
    avg   = total / n
    med   = statistics.median([r["sim_pnl"] for r in bucket])
    wins  = sum(r["win"] for r in bucket)
    out.write(f"{label}:\n")
    out.write(f"  Trades:     {n}\n")
    out.write(f"  Total PnL:  ${total:>+14,.2f}\n")
    out.write(f"  Avg PnL:    ${avg:>+14,.2f}\n")
    out.write(f"  Median PnL: ${med:>+14,.2f}\n")
    out.write(f"  Win rate:   {wins/n:.1%}  ({wins}/{n})\n")
    out.write("\n")

def write_band_row(label, bucket):
    n = len(bucket)
    if n == 0:
        out.write(f"  {label:<25} {n:>4}    ---\n")
        return
    total = sum(r["sim_pnl"] for r in bucket)
    avg   = total / n
    med   = statistics.median([r["sim_pnl"] for r in bucket])
    wins  = sum(r["win"] for r in bucket)
    out.write(f"  {label:<25} {n:>4} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/n:.1%}\n")

def write_quintile(key, sorted_rows, fmt_pct=False):
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
        lo_v  = qb[0][key]
        hi_v  = qb[-1][key]
        total = sum(r["sim_pnl"] for r in qb)
        avg   = total / nq
        med   = statistics.median([r["sim_pnl"] for r in qb])
        wins  = sum(r["win"] for r in qb)
        if fmt_pct:
            out.write(f"  Q{qi+1}      {lo_v:>8.1%} -  {hi_v:>7.1%}   {nq:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/nq:.1%}\n")
        else:
            out.write(f"  Q{qi+1}      {lo_v:>8.2f} -  {hi_v:>7.2f}   {nq:>5} ${total:>+14,.2f} ${avg:>+12,.2f} ${med:>+12,.2f}   {wins/nq:.1%}\n")
    out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 1. OVERALL SUMMARY
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("1. OVERALL SUMMARY (using sim_pnl)\n")
out.write("=" * 80 + "\n\n")
write_summary("All calendar trades", rows)

# ════════════════════════════════════════════════════════════════════════════
# 2. VIX ENTRY vs SIM PNL
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("2. VIX ENTRY vs SIM PNL\n")
out.write("   Question: Does lower VIX at entry lead to higher sim_pnl?\n")
out.write("=" * 80 + "\n\n")

vix_rows = [r for r in rows if r["vix_entry"] is not None]

if len(vix_rows) >= 5:
    out.write("VIX entry quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'VIX Range':<22}       {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>14}    {'Win%'}\n")
    out.write("-" * 100 + "\n")
    sorted_vix = sorted(vix_rows, key=lambda r: r["vix_entry"])
    write_quintile("vix_entry", sorted_vix)

out.write("VIX entry fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

vix_bands = [
    ("VIX < 15",            lambda r: r["vix_entry"] < 15),
    ("15 <= VIX < 20",      lambda r: 15 <= r["vix_entry"] < 20),
    ("20 <= VIX < 25",      lambda r: 20 <= r["vix_entry"] < 25),
    ("25 <= VIX < 30",      lambda r: 25 <= r["vix_entry"] < 30),
    ("VIX >= 30",           lambda r: r["vix_entry"] >= 30),
]
for label, filt in vix_bands:
    b = [r for r in vix_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with VIX entry)", vix_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 3. IV LONG/SHORT RATIO vs SIM PNL
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("3. IV LONG/SHORT RATIO vs SIM PNL\n")
out.write("   Ratio = iv_long_entry / iv_short_entry\n")
out.write("   Question: Does lower ratio lead to higher sim_pnl?\n")
out.write("=" * 80 + "\n\n")

ratio_rows = [r for r in rows if r["iv_ratio"] is not None]

if len(ratio_rows) >= 5:
    out.write("IV ratio quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'Ratio Range':<22}     {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>14}    {'Win%'}\n")
    out.write("-" * 100 + "\n")
    sorted_ratio = sorted(ratio_rows, key=lambda r: r["iv_ratio"])
    write_quintile("iv_ratio", sorted_ratio)

out.write("IV ratio fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

ratio_bands = [
    ("Ratio < 0.90",           lambda r: r["iv_ratio"] < 0.90),
    ("0.90 <= Ratio < 1.00",   lambda r: 0.90 <= r["iv_ratio"] < 1.00),
    ("1.00 <= Ratio < 1.10",   lambda r: 1.00 <= r["iv_ratio"] < 1.10),
    ("1.10 <= Ratio < 1.20",   lambda r: 1.10 <= r["iv_ratio"] < 1.20),
    ("Ratio >= 1.20",          lambda r: r["iv_ratio"] >= 1.20),
]
for label, filt in ratio_bands:
    b = [r for r in ratio_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with IV ratio)", ratio_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 4. |STOCK CHANGE %| vs SIM PNL
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("4. |STOCK CHANGE %| vs SIM PNL\n")
out.write("   Question: Does higher absolute stock move lead to lower sim_pnl?\n")
out.write("=" * 80 + "\n\n")

stk_rows = [r for r in rows if r["abs_stk_chg"] is not None]

if len(stk_rows) >= 5:
    out.write("|Stk Chg%| quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'|Chg%| Range':<22}     {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>14}    {'Win%'}\n")
    out.write("-" * 100 + "\n")
    sorted_stk = sorted(stk_rows, key=lambda r: r["abs_stk_chg"])
    write_quintile("abs_stk_chg", sorted_stk)

out.write("|Stk Chg%| fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

abs_bands = [
    ("|Chg| < 2%",            lambda r: r["abs_stk_chg"] < 2),
    ("2% <= |Chg| < 5%",      lambda r: 2 <= r["abs_stk_chg"] < 5),
    ("5% <= |Chg| < 10%",     lambda r: 5 <= r["abs_stk_chg"] < 10),
    ("10% <= |Chg| < 20%",    lambda r: 10 <= r["abs_stk_chg"] < 20),
    ("|Chg| >= 20%",          lambda r: r["abs_stk_chg"] >= 20),
]
for label, filt in abs_bands:
    b = [r for r in stk_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with Stk Chg%)", stk_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 5. TOP 100 LOSING & WINNING TRADES
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("5. TOP 100 LOSING & WINNING TRADES (by sim_pnl)\n")
out.write("=" * 80 + "\n")

sorted_by_sim = sorted(rows, key=lambda r: r["sim_pnl"])

def write_trade_table(label, trade_list):
    out.write(f"\n{label}:\n")
    hdr = (f"  {'Ticker':<7} {'Earnings':>10}"
           f" {'VIXen':>7} {'VIXex':>7}"
           f" {'IVratio':>8}"
           f" {'StkChg%':>8}"
           f" {'LongPnL':>14} {'ShortPnL':>14} {'StkPnL':>14}"
           f" {'SimPnL':>14}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    for r in trade_list:
        out.write(
            f"  {r['ticker']:<7} {r['earnings']:>10}"
            f" {_fmt(r['vix_entry'], '.1f'):>7}"
            f" {_fmt(r['vix_exit'], '.1f'):>7}"
            f" {_fmt(r['iv_ratio'], '.3f'):>8}"
            f" {_fmt(r['stk_chg_pct'], '+.1f', '%'):>8}"
            f" ${r['long_pnl']:>+12,.2f}"
            f" ${r['short_pnl']:>+12,.2f}"
            f" ${r['stk_pnl']:>+12,.2f}"
            f" ${r['sim_pnl']:>+12,.2f}"
            f"\n"
        )

write_trade_table("TOP 100 WORST TRADES", sorted_by_sim[:100])
write_trade_table("TOP 100 BEST TRADES", sorted_by_sim[-100:][::-1])
out.write("\n")

out.close()
print(f"Analysis -> {args.output}  ({len(rows)} trades)")
