#!/usr/bin/env python3
"""
Analyse calendar-spread trades CSV (output of parse_log.py).

Focuses on: IV entry, ShIV/RV, and IVspread effects on profitability.
Also includes a monthly P&L breakdown (filtered: IV/RV < 1.5 AND VIX at entry < 25)
using sim_pnl, with Trades / Wins / Win% / Avg / Total / Cumulative columns.

Usage:
    python analyze_cal_trades.py trades.csv                → cal-stats.out
    python analyze_cal_trades.py trades.csv -o myfile.out  → custom output
"""
import csv, sys, re, statistics, argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description="Analyse calendar-spread trades CSV")
parser.add_argument("csvfile", help="Input trades CSV (from parse_log.py)")
parser.add_argument("-o", "--output", default=None,
                    help="Output file path (default: cal-stats.out)")
parser.add_argument("--log", default=None,
                    help="Optional: raw QC log file to extract skip counts from")
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
        iv_change  = pct(row.get("iv_change", ""))
        vix_entry  = parse_float(row.get("vix_entry", ""))
        sim_pnl    = money(row.get("sim_pnl", ""))

        n_contracts = int(row["n_contracts"]) if row.get("n_contracts", "").strip() else 0

        _lse = int(row["long_spread_entry"])  if row.get("long_spread_entry",  "").strip() else 0
        _sse = int(row["short_spread_entry"]) if row.get("short_spread_entry", "").strip() else 0
        _lsx = int(row["long_spread_exit"])   if row.get("long_spread_exit",   "").strip() else 0
        _ssx = int(row["short_spread_exit"])  if row.get("short_spread_exit",  "").strip() else 0

        rows.append({
            "ticker":      row["ticker"],
            "earnings":    row["earnings"],
            "n_contracts": n_contracts,
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
            "iv_change":   iv_change,
            "vix_entry":   vix_entry,
            "sim_pnl":     sim_pnl,
            "win":         win,
            "long_spread_entry":  _lse,
            "short_spread_entry": _sse,
            "long_spread_exit":   _lsx,
            "short_spread_exit":  _ssx,
        })

# ── Parse skip counts from raw log (optional) ────────────────────────────────

_skip_totals_re = re.compile(
    r'SKIP TOTALS:\s*(\d+)\s+attempted\s*\|\s*(\d+)\s+traded\s*\|\s*(\d+)\s+skipped\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)
_skip_ticker_re = re.compile(
    r'Entries attempted:\s*(\d+)\s*\|\s*Skipped:\s*(\d+)\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)
_summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

skip_grand   = None     # dict with grand totals
skip_by_ticker = {}     # ticker → {attempted, skipped, no_pair, low_debit, other}

if args.log:
    try:
        with open(args.log, "r", encoding="utf-8", errors="replace") as lf:
            _cur_tkr = None
            for ln in lf:
                m = _summary_re.search(ln)
                if m:
                    _cur_tkr = m.group(1)
                m = _skip_ticker_re.search(ln)
                if m and _cur_tkr:
                    skip_by_ticker[_cur_tkr] = {
                        "attempted": int(m.group(1)), "skipped": int(m.group(2)),
                        "no_pair": int(m.group(3)), "low_debit": int(m.group(4)),
                        "other": int(m.group(5)),
                    }
                m = _skip_totals_re.search(ln)
                if m:
                    skip_grand = {
                        "attempted": int(m.group(1)), "traded": int(m.group(2)),
                        "skipped": int(m.group(3)), "no_pair": int(m.group(4)),
                        "low_debit": int(m.group(5)), "other": int(m.group(6)),
                    }
    except Exception as e:
        print(f"Warning: could not read log file {args.log}: {e}")

out = open(out_path, "w", encoding="utf-8")
out.write(f"Total calendar trades: {len(rows)}\n")

# ── Skip summary at top of report ─────────────────────────────────────────
if skip_grand:
    g = skip_grand
    out.write(f"\nENTRY SKIP SUMMARY ({g['attempted']} earnings events attempted):\n")
    out.write(f"  Traded:     {g['traded']:>5}\n")
    out.write(f"  Skipped:    {g['skipped']:>5}  "
              f"(no_pair={g['no_pair']}, low_debit={g['low_debit']}, other={g['other']})\n")
    if g['attempted'] > 0:
        out.write(f"  Trade rate: {g['traded']/g['attempted']:.1%}\n")
    out.write(f"\n  no_pair    = no weekly options or no valid ATM pair\n")
    out.write(f"  low_debit  = net debit <= 0 or < MIN_NET_DEBIT\n")
    out.write(f"  other      = bad price / MAX_PUT_PCT exceeded / IV/RV filter\n")
out.write("\n")

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
    tot_l   = sum(r["long_pnl"] for r in bucket)
    tot_s   = sum(r["short_pnl"] for r in bucket)
    tot_stk = sum(r["stk_pnl"] for r in bucket)
    avg_l   = tot_l / n
    avg_s   = tot_s / n
    avg_stk = tot_stk / n
    out.write(f"{label}:\n")
    out.write(f"  Trades:         {n}\n")
    out.write(f"  Total PnL:      ${total:>+14,.2f}\n")
    out.write(f"  Avg PnL:        ${avg:>+14,.2f}\n")
    out.write(f"  Median PnL:     ${med:>+14,.2f}\n")
    out.write(f"  Win rate:       {wins/n:.1%}  ({wins}/{n})\n")
    out.write(f"  Total Long PnL: ${tot_l:>+14,.2f}\n")
    out.write(f"  Total Short PnL:${tot_s:>+14,.2f}\n")
    out.write(f"  Total Stk PnL:  ${tot_stk:>+14,.2f}\n")
    out.write(f"  ---\n")
    out.write(f"  Avg Long PnL:   ${avg_l:>+14,.2f}\n")
    out.write(f"  Avg Short PnL:  ${avg_s:>+14,.2f}\n")
    out.write(f"  Avg Stk PnL:    ${avg_stk:>+14,.2f}\n")
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
# 1b. TOP 10 BEST & WORST TRADES
# ════════════════════════════════════════════════════════════════════════════

def _fmt(v, fmt_str, suffix="", na="n/a"):
    """Format a value, returning 'n/a' if None."""
    if v is None:
        return na.rjust(len(na))
    return f"{v:{fmt_str}}{suffix}"

def write_top_table(label, trade_list):
    out.write(f"\n{label}:\n")
    hdr = (f"  {'Ticker':<7} {'Earnings':>10} {'n':>5}  {'Long PnL':>12} {'Short PnL':>12} "
           f"{'Stock PnL':>12} {'StkChg%':>8} {'Combined':>12}  "
           f"{'IVent':>7} {'IVexit':>7} {'IVspr':>7} {'ShIV/RV':>7} {'IVchg':>7} {'IV/RV':>6}"
           f" {'LSpEn':>7} {'SSpEn':>7} {'LSpEx':>7} {'SSpEx':>7}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    for r in trade_list:
        nc = r.get("n_contracts", 0)
        out.write(
            f"  {r['ticker']:<7} {r['earnings']:>10} {nc:>5}"
            f"  ${r['long_pnl']:>+11,.0f} ${r['short_pnl']:>+11,.0f}"
            f" ${r['stk_pnl']:>+11,.0f}"
            f"  {_fmt(r['stk_chg_pct'], '+.1f', '%'):>7}"
            f" ${r['combined']:>+11,.0f}"
            f"  {_fmt(r['iv_entry'], '.1%'):>7}"
            f" {_fmt(r['iv_exit'], '.1%'):>7}"
            f" {_fmt(r['ivspread'], '.1%'):>7}"
            f" {_fmt(r['shiv_rv'], '.2f'):>7}"
            f" {_fmt(r['iv_change'], '+.0%'):>7}"
            f" {_fmt(r['iv_rv'], '.2f'):>6}"
            f" {r.get('long_spread_entry', 0):>7}"
            f" {r.get('short_spread_entry', 0):>7}"
            f" {r.get('long_spread_exit', 0):>7}"
            f" {r.get('short_spread_exit', 0):>7}"
            f"\n"
        )

sorted_by_pnl = sorted(rows, key=lambda r: r["combined"])
write_top_table("TOP 50 WORST TRADES", sorted_by_pnl[:50])
write_top_table("TOP 50 BEST TRADES", sorted_by_pnl[-50:][::-1])
out.write("\n")

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

# ════════════════════════════════════════════════════════════════════════════
# 8. MONTHLY PnL (filtered: IV/RV < 1.5 AND VIX at entry < 25)
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("8. MONTHLY PnL  (filtered: IV/RV < 1.5 AND VIX at entry < 25)\n")
out.write("=" * 80 + "\n\n")

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
monthly_data = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
monthly_skipped = 0

for r in rows:
    if r["iv_rv"] is None or r["vix_entry"] is None:
        monthly_skipped += 1
        continue
    ym_match = re.match(r"(\d{4})-(\d{2})", r["earnings"])
    if not ym_match:
        monthly_skipped += 1
        continue
    if r["iv_rv"] < 1.5 and r["vix_entry"] < 25:
        ym_key = ym_match.group(0)
        monthly_data[ym_key]["pnl"] += r["sim_pnl"]
        monthly_data[ym_key]["trades"] += 1
        if r["win"]:
            monthly_data[ym_key]["wins"] += 1

if monthly_data:
    all_yms = sorted(monthly_data.keys())
    first_year = int(all_yms[0][:4])
    last_year = int(all_yms[-1][:4])

    hdr = (f"  {'Month':<12} {'Trades':>6} {'Wins':>5} {'Win%':>6} "
           f"{'Avg PnL':>10} {'Total PnL':>12} {'Cumulative':>12}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")

    cumulative = 0.0
    total_trades = 0
    total_wins = 0
    total_pnl = 0.0
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}"
            label = f"{year} {MONTH_NAMES[month - 1]}"
            d = monthly_data.get(ym)
            if d and d["trades"] > 0:
                cumulative += d["pnl"]
                total_trades += d["trades"]
                total_wins += d["wins"]
                total_pnl += d["pnl"]
                win_pct = d["wins"] / d["trades"] * 100
                avg_pnl = d["pnl"] / d["trades"]
                out.write(
                    f"  {label:<12} {d['trades']:>6} {d['wins']:>5} "
                    f"{win_pct:>5.1f}% {avg_pnl:>+10,.0f} "
                    f"{d['pnl']:>+12,.0f} {cumulative:>+12,.0f}\n"
                )
            else:
                out.write(f"  {label:<12} {'0':>6}\n")

    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    if total_trades > 0:
        overall_win_pct = total_wins / total_trades * 100
        overall_avg = total_pnl / total_trades
        out.write(
            f"  {'TOTAL':<12} {total_trades:>6} {total_wins:>5} "
            f"{overall_win_pct:>5.1f}% {overall_avg:>+10,.0f} "
            f"{total_pnl:>+12,.0f} {cumulative:>+12,.0f}\n"
        )
    out.write(f"\n  Skipped (missing iv_rv/vix/earnings): {monthly_skipped}\n")
else:
    out.write("  (no valid data — need iv_rv, vix_entry, sim_pnl, earnings columns)\n")

out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 9. PER-TICKER SKIP BREAKDOWN (from --log)
# ════════════════════════════════════════════════════════════════════════════

if skip_by_ticker:
    out.write("=" * 80 + "\n")
    out.write("9. PER-TICKER ENTRY SKIP BREAKDOWN\n")
    out.write("=" * 80 + "\n\n")
    out.write(f"  {'Ticker':<8} {'Attempted':>9} {'Traded':>7} {'Skipped':>8}  "
              f"{'no_pair':>8} {'low_dbt':>8} {'other':>6}\n")
    out.write("  " + "-" * 62 + "\n")
    for tkr in sorted(skip_by_ticker.keys()):
        s = skip_by_ticker[tkr]
        traded = s["attempted"] - s["skipped"]
        out.write(f"  {tkr:<8} {s['attempted']:>9} {traded:>7} {s['skipped']:>8}  "
                  f"{s['no_pair']:>8} {s['low_debit']:>8} {s['other']:>6}\n")
    out.write("  " + "-" * 62 + "\n")
    if skip_grand:
        g = skip_grand
        out.write(f"  {'TOTAL':<8} {g['attempted']:>9} {g['traded']:>7} {g['skipped']:>8}  "
                  f"{g['no_pair']:>8} {g['low_debit']:>8} {g['other']:>6}\n")
    out.write("\n")

out.close()
print(f"Analysis -> {out_path}  ({len(rows)} calendar trades)")
