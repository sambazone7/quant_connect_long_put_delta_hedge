#!/usr/bin/env python3
"""
Analyse calendar-spread trades CSV for sim_pnl relationships.

Focuses on: VIX entry, IV long/short ratio, |stock change %|, and short-leg IV vs sim_pnl.
Also includes a monthly P&L breakdown (filtered: IV/RV < 1.5 AND VIX at entry < 25)
using sim_pnl, with Trades / Wins / Win% / Avg / Total / Cumulative columns.

Usage:
    python cal_analyze_sim.py <input.csv> <output.out>
"""
import csv, sys, re, statistics, argparse
from collections import defaultdict

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
        iv_long_exit = pct(row.get("iv_long_exit", ""))
        iv_short = pct(row.get("iv_short_entry", ""))
        stk_chg = parse_signed(row.get("stk_chg_pct", ""))
        stk_max_up = parse_signed(row.get("stk_max_up_pct", ""))
        stk_max_dn = parse_signed(row.get("stk_max_dn_pct", ""))
        stk_pnl   = money(row.get("stock_pnl", ""))
        long_pnl  = money(row.get("long_pnl", ""))
        short_pnl = money(row.get("short_pnl", ""))
        iv_rv     = parse_float(row.get("iv_rv", ""))
        n_cals    = int(row["n_calendars"]) if row.get("n_calendars", "").strip() else (
                    int(row["n_contracts"]) if row.get("n_contracts", "").strip() else 0)

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
            "iv_long":      iv_long,
            "iv_long_exit": iv_long_exit,
            "iv_short":     iv_short,
            "iv_ratio":    iv_ratio,
            "stk_chg_pct":    stk_chg,
            "stk_max_up_pct": stk_max_up,
            "stk_max_dn_pct": stk_max_dn,
            "stk_pnl":        stk_pnl,
            "long_pnl":    long_pnl,
            "short_pnl":   short_pnl,
            "iv_rv":       iv_rv,
            "n_cals":      n_cals,
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

def write_ticker_rank_table(label, ranked_list):
    """Render a per-ticker rankings table. ranked_list is already sorted; each
    record carries: ticker, n, wins, total, avg, median, best, worst,
    avg_iv_short."""
    out.write(f"\n{label}:\n")
    hdr = (f"  {'Rank':>4}  {'Ticker':<7}  {'Trades':>6}  {'Wins':>4}  {'Win%':>6}  "
           f"{'AvgShIV':>8}  "
           f"{'Total SimPnL':>16}  {'Avg SimPnL':>14}  {'Median SimPnL':>15}  "
           f"{'Best Trade':>14}  {'Worst Trade':>14}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    for i, r in enumerate(ranked_list, 1):
        win_pct = (r["wins"] / r["n"] * 100) if r["n"] > 0 else 0.0
        sh_iv   = f"{r['avg_iv_short']:>7.1%}" if r["avg_iv_short"] is not None else f"{'n/a':>8}"
        out.write(
            f"  {i:>4}  {r['ticker']:<7}  {r['n']:>6}  {r['wins']:>4}  {win_pct:>5.1f}%  "
            f"{sh_iv:>8}  "
            f"${r['total']:>+15,.2f}  ${r['avg']:>+13,.2f}  ${r['median']:>+14,.2f}  "
            f"${r['best']:>+13,.2f}  ${r['worst']:>+13,.2f}\n"
        )

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
# 5. SHORT LEG IV ENTRY vs SIM PNL
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("5. SHORT LEG IV ENTRY vs SIM PNL\n")
out.write("   Question: Does lower short-leg IV at entry lead to higher sim_pnl?\n")
out.write("=" * 80 + "\n\n")

iv_short_rows = [r for r in rows if r["iv_short"] is not None]

if len(iv_short_rows) >= 5:
    out.write("Short IV entry quintile analysis:\n")
    out.write(f"{'Quintile':<12} {'ShortIV Range':<22}     {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>14}    {'Win%'}\n")
    out.write("-" * 100 + "\n")
    sorted_iv_short = sorted(iv_short_rows, key=lambda r: r["iv_short"])
    write_quintile("iv_short", sorted_iv_short, fmt_pct=True)

out.write("Short IV entry fixed-band analysis:\n")
out.write(f"{'Band':<25}   {'N':>5} {'Total SimPnL':>16} {'Avg SimPnL':>12} {'Median':>12}    {'Win%'}\n")
out.write("-" * 85 + "\n")

iv_short_bands = [
    ("ShIV < 20%",             lambda r: r["iv_short"] is not None and r["iv_short"] < 0.20),
    ("20% <= ShIV < 30%",      lambda r: r["iv_short"] is not None and 0.20 <= r["iv_short"] < 0.30),
    ("30% <= ShIV < 40%",      lambda r: r["iv_short"] is not None and 0.30 <= r["iv_short"] < 0.40),
    ("40% <= ShIV < 50%",      lambda r: r["iv_short"] is not None and 0.40 <= r["iv_short"] < 0.50),
    ("50% <= ShIV < 60%",      lambda r: r["iv_short"] is not None and 0.50 <= r["iv_short"] < 0.60),
    ("ShIV >= 60%",            lambda r: r["iv_short"] is not None and r["iv_short"] >= 0.60),
]
for label, filt in iv_short_bands:
    b = [r for r in iv_short_rows if filt(r)]
    write_band_row(label, b)
out.write("-" * 85 + "\n")
write_band_row("ALL (with ShortIV)", iv_short_rows)
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 6. TOP 100 LOSING & WINNING TRADES
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("6. TOP 100 LOSING & WINNING TRADES (by sim_pnl)\n")
out.write("=" * 80 + "\n")

sorted_by_sim = sorted(rows, key=lambda r: r["sim_pnl"])

def write_trade_table(label, trade_list):
    out.write(f"\n{label}:\n")
    hdr = (f"  {'Ticker':<7} {'nCal':>10}"
           f" {'IVLen':>7} {'IVLex':>7}"
           f" {'VIXen':>7} {'VIXex':>7}"
           f" {'IVratio':>8}"
           f" {'MaxUp%':>8} {'MaxDn%':>8} {'StkChg%':>8}"
           f" {'LongPnL':>14} {'ShortPnL':>14} {'OptPnL':>14} {'StkPnL':>14}"
           f" {'SimPnL':>14}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    for r in trade_list:
        out.write(
            f"  {r['ticker']:<7} {r['n_cals']:>10}"
            f" {_fmt(r['iv_long'], '.1%'):>7}"
            f" {_fmt(r['iv_long_exit'], '.1%'):>7}"
            f" {_fmt(r['vix_entry'], '.1f'):>7}"
            f" {_fmt(r['vix_exit'], '.1f'):>7}"
            f" {_fmt(r['iv_ratio'], '.3f'):>8}"
            f" {_fmt(r['stk_max_up_pct'], '+.1f', '%'):>8}"
            f" {_fmt(r['stk_max_dn_pct'], '+.1f', '%'):>8}"
            f" {_fmt(r['stk_chg_pct'], '+.1f', '%'):>8}"
            f" ${r['long_pnl']:>+12,.2f}"
            f" ${r['short_pnl']:>+12,.2f}"
            f" ${r['long_pnl'] + r['short_pnl']:>+12,.2f}"
            f" ${r['stk_pnl']:>+12,.2f}"
            f" ${r['sim_pnl']:>+12,.2f}"
            f"\n"
        )

write_trade_table("TOP 100 WORST TRADES", sorted_by_sim[:100])
write_trade_table("TOP 100 BEST TRADES", sorted_by_sim[-100:][::-1])
out.write("\n")

# ════════════════════════════════════════════════════════════════════════════
# 7. MONTHLY PnL (filtered: IV/RV < 1.5 AND VIX at entry < 25)
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("7. MONTHLY PnL  (filtered: IV/RV < 1.5 AND VIX at entry < 25)\n")
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
    last_year  = int(all_yms[-1][:4])

    hdr = (f"  {'Month':<12} {'Trades':>6} {'Wins':>5} {'Win%':>6} "
           f"{'Avg PnL':>10} {'Total PnL':>12} {'Cumulative':>12}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")

    cumulative   = 0.0
    total_trades = 0
    total_wins   = 0
    total_pnl    = 0.0
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}"
            label = f"{year} {MONTH_NAMES[month - 1]}"
            d = monthly_data.get(ym)
            if d and d["trades"] > 0:
                cumulative   += d["pnl"]
                total_trades += d["trades"]
                total_wins   += d["wins"]
                total_pnl    += d["pnl"]
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
        overall_avg     = total_pnl / total_trades
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
# 8. TICKER RANKINGS BY TOTAL SIM_PNL
# ════════════════════════════════════════════════════════════════════════════

out.write("=" * 80 + "\n")
out.write("8. TICKER RANKINGS BY TOTAL SIM_PNL\n")
out.write("   Aggregated across all trades per ticker.  Sort key: total sim_pnl.\n")
out.write("=" * 80 + "\n\n")

ticker_stats = defaultdict(lambda: {"n": 0, "wins": 0, "total": 0.0,
                                    "pnls": [], "iv_shorts": []})
for r in rows:
    t = r["ticker"]
    if not t:
        continue
    ticker_stats[t]["n"]     += 1
    ticker_stats[t]["wins"]  += 1 if r["win"] else 0
    ticker_stats[t]["total"] += r["sim_pnl"]
    ticker_stats[t]["pnls"].append(r["sim_pnl"])
    if r["iv_short"] is not None:
        ticker_stats[t]["iv_shorts"].append(r["iv_short"])

ticker_records = [{
    "ticker":       t,
    "n":            d["n"],
    "wins":         d["wins"],
    "total":        d["total"],
    "avg":          d["total"] / d["n"],
    "median":       statistics.median(d["pnls"]),
    "best":         max(d["pnls"]),
    "worst":        min(d["pnls"]),
    "avg_iv_short": (sum(d["iv_shorts"]) / len(d["iv_shorts"])) if d["iv_shorts"] else None,
} for t, d in ticker_stats.items()]

if ticker_records:
    out.write(f"Universe: {len(ticker_records)} unique tickers, {len(rows)} trades total\n")

    best_50  = sorted(ticker_records, key=lambda r: r["total"], reverse=True)[:50]
    worst_50 = sorted(ticker_records, key=lambda r: r["total"])[:50]

    write_ticker_rank_table("TOP 50 BEST TICKERS (by total sim_pnl)",  best_50)
    write_ticker_rank_table("TOP 50 WORST TICKERS (by total sim_pnl)", worst_50)
else:
    out.write("  (no ticker data available)\n")

out.write("\n")

out.close()
print(f"Analysis -> {args.output}  ({len(rows)} trades)")
