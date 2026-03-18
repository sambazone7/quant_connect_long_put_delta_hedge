#!/usr/bin/env python3
"""
Analyze single-put earnings trade CSV output from parse_log.py.

Uses sim_pnl as the primary profit metric.

Usage:
    python analyze_put_trades.py <trades.csv>
"""
import csv, sys, argparse, math
from collections import defaultdict

parser = argparse.ArgumentParser(description="Analyze single-put earnings trades")
parser.add_argument("csvfile", help="Input CSV from parse_log.py")
args = parser.parse_args()

# ── Load trades ───────────────────────────────────────────────────────────────

trades = []
with open(args.csvfile, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get("long_pnl"):
            continue
        if not row.get("sim_pnl"):
            continue
        t = {}
        t["ticker"]    = row["ticker"]
        t["win"]       = row["win"]
        t["earnings"]  = row["earnings"]
        t["put_pnl"]   = float(row.get("put_pnl") or 0)
        t["stock_pnl"] = float(row.get("stock_pnl") or 0)
        t["stk_chg"]   = float(row.get("stk_chg_pct") or 0)
        t["combined"]  = float(row.get("combined") or 0)
        t["sim_pnl"]   = float(row["sim_pnl"])

        iv_d = row.get("iv_diff", "").replace("%", "").strip()
        t["iv_diff"] = float(iv_d) if iv_d else None

        ne = row.get("non_earn_iv_rv", "").replace("%", "").strip()
        t["ne_iv_rv"] = float(ne) if ne else None

        iv_e = row.get("iv_entry", "").replace("%", "").strip()
        t["iv_entry"] = float(iv_e) if iv_e else None

        iv_x = row.get("iv_exit", "").replace("%", "").strip()
        t["iv_exit"] = float(iv_x) if iv_x else None

        iv_rv = row.get("iv_rv", "").strip()
        t["iv_rv"] = float(iv_rv) if iv_rv else None

        trades.append(t)

if not trades:
    print("No single-put trades with sim_pnl found.")
    sys.exit(0)

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt(v):
    return f"${v:+,.0f}"

def pct(v):
    return f"{v:.1f}%"

def stats(subset, label=""):
    if not subset:
        return
    n = len(subset)
    total = sum(t["sim_pnl"] for t in subset)
    wins  = sum(1 for t in subset if t["sim_pnl"] > 0)
    avg   = total / n
    print(f"  {label:40s}  n={n:4d}   total={fmt(total):>12s}   avg={fmt(avg):>10s}   win%={wins/n*100:5.1f}%")

SEP = "=" * 100

# ── 1. Overall Summary ───────────────────────────────────────────────────────

print(f"\n{SEP}")
print("  OVERALL SUMMARY")
print(SEP)
total_pnl = sum(t["sim_pnl"] for t in trades)
wins = sum(1 for t in trades if t["sim_pnl"] > 0)
avg_pnl = total_pnl / len(trades)
print(f"  Trades: {len(trades)}   Wins: {wins}   Losses: {len(trades)-wins}   Win%: {wins/len(trades)*100:.1f}%")
print(f"  Total SimPnL: {fmt(total_pnl)}   Avg SimPnL: {fmt(avg_pnl)}")
print(f"  Best:  {fmt(max(t['sim_pnl'] for t in trades))}   Worst: {fmt(min(t['sim_pnl'] for t in trades))}")

# ── 2. Top / Worst Trades ────────────────────────────────────────────────────

print(f"\n{SEP}")
print("  TOP 15 TRADES (by SimPnL)")
print(SEP)
for t in sorted(trades, key=lambda x: x["sim_pnl"], reverse=True)[:15]:
    extras = []
    if t["iv_diff"] is not None: extras.append(f"IVdif={pct(t['iv_diff'])}")
    if t["iv_entry"] is not None: extras.append(f"IVen={pct(t['iv_entry'])}")
    if t["iv_rv"] is not None: extras.append(f"IV/RV={t['iv_rv']:.2f}")
    if t["ne_iv_rv"] is not None: extras.append(f"neIVR={t['ne_iv_rv']:.2f}")
    print(f"  {t['ticker']:6s} {t['earnings']}  SimPnL={fmt(t['sim_pnl']):>10s}  StkChg={t['stk_chg']:+.1f}%  {' '.join(extras)}")

print(f"\n{SEP}")
print("  WORST 15 TRADES (by SimPnL)")
print(SEP)
for t in sorted(trades, key=lambda x: x["sim_pnl"])[:15]:
    extras = []
    if t["iv_diff"] is not None: extras.append(f"IVdif={pct(t['iv_diff'])}")
    if t["iv_entry"] is not None: extras.append(f"IVen={pct(t['iv_entry'])}")
    if t["iv_rv"] is not None: extras.append(f"IV/RV={t['iv_rv']:.2f}")
    if t["ne_iv_rv"] is not None: extras.append(f"neIVR={t['ne_iv_rv']:.2f}")
    print(f"  {t['ticker']:6s} {t['earnings']}  SimPnL={fmt(t['sim_pnl']):>10s}  StkChg={t['stk_chg']:+.1f}%  {' '.join(extras)}")

# ── 3. IVdif Buckets ─────────────────────────────────────────────────────────

iv_diff_trades = [t for t in trades if t["iv_diff"] is not None]
if iv_diff_trades:
    print(f"\n{SEP}")
    print("  ANALYSIS: IVdif (IV term-structure spread)")
    print(SEP)
    buckets = [
        ("IVdif < 2%",    lambda t: t["iv_diff"] < 2),
        ("IVdif 2-4%",    lambda t: 2 <= t["iv_diff"] < 4),
        ("IVdif 4-6%",    lambda t: 4 <= t["iv_diff"] < 6),
        ("IVdif 6-8%",    lambda t: 6 <= t["iv_diff"] < 8),
        ("IVdif >= 8%",   lambda t: t["iv_diff"] >= 8),
    ]
    for label, fn in buckets:
        stats([t for t in iv_diff_trades if fn(t)], label)

# ── 4. IV Entry Buckets ──────────────────────────────────────────────────────

iv_entry_trades = [t for t in trades if t["iv_entry"] is not None]
if iv_entry_trades:
    print(f"\n{SEP}")
    print("  ANALYSIS: IV at Entry")
    print(SEP)
    buckets = [
        ("IV entry < 25%",   lambda t: t["iv_entry"] < 25),
        ("IV entry 25-35%",  lambda t: 25 <= t["iv_entry"] < 35),
        ("IV entry 35-45%",  lambda t: 35 <= t["iv_entry"] < 45),
        ("IV entry 45-60%",  lambda t: 45 <= t["iv_entry"] < 60),
        ("IV entry >= 60%",  lambda t: t["iv_entry"] >= 60),
    ]
    for label, fn in buckets:
        stats([t for t in iv_entry_trades if fn(t)], label)

# ── 5. Combined IVdif + IV Entry ─────────────────────────────────────────────

both_trades = [t for t in trades if t["iv_diff"] is not None and t["iv_entry"] is not None]
if both_trades:
    print(f"\n{SEP}")
    print("  ANALYSIS: IVdif + IV Entry Combined")
    print(SEP)
    combos = [
        ("Low IVdif(<4%) + Low IV(<30%)",   lambda t: t["iv_diff"] < 4 and t["iv_entry"] < 30),
        ("Low IVdif(<4%) + Mid IV(30-45%)", lambda t: t["iv_diff"] < 4 and 30 <= t["iv_entry"] < 45),
        ("Low IVdif(<4%) + High IV(>=45%)", lambda t: t["iv_diff"] < 4 and t["iv_entry"] >= 45),
        ("High IVdif(>=4%) + Low IV(<30%)", lambda t: t["iv_diff"] >= 4 and t["iv_entry"] < 30),
        ("High IVdif(>=4%) + Mid IV(30-45%)", lambda t: t["iv_diff"] >= 4 and 30 <= t["iv_entry"] < 45),
        ("High IVdif(>=4%) + High IV(>=45%)", lambda t: t["iv_diff"] >= 4 and t["iv_entry"] >= 45),
    ]
    for label, fn in combos:
        stats([t for t in both_trades if fn(t)], label)

# ── 6. IV/RV Buckets ─────────────────────────────────────────────────────────

ivrv_trades = [t for t in trades if t["iv_rv"] is not None]
if ivrv_trades:
    print(f"\n{SEP}")
    print("  ANALYSIS: IV/RV Ratio at Entry")
    print(SEP)
    buckets = [
        ("IV/RV < 0.8",    lambda t: t["iv_rv"] < 0.8),
        ("IV/RV 0.8-1.0",  lambda t: 0.8 <= t["iv_rv"] < 1.0),
        ("IV/RV 1.0-1.2",  lambda t: 1.0 <= t["iv_rv"] < 1.2),
        ("IV/RV 1.2-1.5",  lambda t: 1.2 <= t["iv_rv"] < 1.5),
        ("IV/RV >= 1.5",   lambda t: t["iv_rv"] >= 1.5),
    ]
    for label, fn in buckets:
        stats([t for t in ivrv_trades if fn(t)], label)

# ── 7. Non-Earnings IV/RV (neIVR) Buckets ────────────────────────────────────

ne_trades = [t for t in trades if t["ne_iv_rv"] is not None]
if ne_trades:
    print(f"\n{SEP}")
    print("  ANALYSIS: Non-Earnings IV/RV (neIVR)")
    print(SEP)
    buckets = [
        ("neIVR < 0.7",    lambda t: t["ne_iv_rv"] < 0.7),
        ("neIVR 0.7-0.9",  lambda t: 0.7 <= t["ne_iv_rv"] < 0.9),
        ("neIVR 0.9-1.1",  lambda t: 0.9 <= t["ne_iv_rv"] < 1.1),
        ("neIVR 1.1-1.3",  lambda t: 1.1 <= t["ne_iv_rv"] < 1.3),
        ("neIVR >= 1.3",   lambda t: t["ne_iv_rv"] >= 1.3),
    ]
    for label, fn in buckets:
        stats([t for t in ne_trades if fn(t)], label)

# ── 8. Stock Change % vs Profit ──────────────────────────────────────────────

print(f"\n{SEP}")
print("  ANALYSIS: Absolute Stock Change % vs SimPnL")
print(SEP)
buckets = [
    ("|StkChg| < 2%",    lambda t: abs(t["stk_chg"]) < 2),
    ("|StkChg| 2-5%",    lambda t: 2 <= abs(t["stk_chg"]) < 5),
    ("|StkChg| 5-10%",   lambda t: 5 <= abs(t["stk_chg"]) < 10),
    ("|StkChg| 10-15%",  lambda t: 10 <= abs(t["stk_chg"]) < 15),
    ("|StkChg| >= 15%",  lambda t: abs(t["stk_chg"]) >= 15),
]
for label, fn in buckets:
    stats([t for t in trades if fn(t)], label)

# ── 9. Per-Ticker Summary ────────────────────────────────────────────────────

print(f"\n{SEP}")
print("  PER-TICKER SUMMARY (sorted by avg SimPnL)")
print(SEP)
print(f"  {'Ticker':8s} {'Trades':>6s} {'Wins':>5s} {'Win%':>6s} {'TotalSimPnL':>13s} {'AvgSimPnL':>11s} {'AvgIVdif':>9s} {'AvgIV/RV':>9s} {'AvgneIVR':>9s}")
print(f"  {'-'*8} {'-'*6} {'-'*5} {'-'*6} {'-'*13} {'-'*11} {'-'*9} {'-'*9} {'-'*9}")

ticker_data = defaultdict(list)
for t in trades:
    ticker_data[t["ticker"]].append(t)

ticker_rows = []
for ticker, tlist in ticker_data.items():
    n = len(tlist)
    total = sum(t["sim_pnl"] for t in tlist)
    avg = total / n
    w = sum(1 for t in tlist if t["sim_pnl"] > 0)
    wp = w / n * 100

    ivd_vals = [t["iv_diff"] for t in tlist if t["iv_diff"] is not None]
    avg_ivd = sum(ivd_vals) / len(ivd_vals) if ivd_vals else None

    ivrv_vals = [t["iv_rv"] for t in tlist if t["iv_rv"] is not None]
    avg_ivrv = sum(ivrv_vals) / len(ivrv_vals) if ivrv_vals else None

    ne_vals = [t["ne_iv_rv"] for t in tlist if t["ne_iv_rv"] is not None]
    avg_ne = sum(ne_vals) / len(ne_vals) if ne_vals else None

    ticker_rows.append((ticker, n, w, wp, total, avg, avg_ivd, avg_ivrv, avg_ne))

for row in sorted(ticker_rows, key=lambda x: x[5], reverse=True):
    ticker, n, w, wp, total, avg, avg_ivd, avg_ivrv, avg_ne = row
    ivd_s  = f"{avg_ivd:.1f}%" if avg_ivd is not None else "n/a"
    ivrv_s = f"{avg_ivrv:.2f}" if avg_ivrv is not None else "n/a"
    ne_s   = f"{avg_ne:.2f}" if avg_ne is not None else "n/a"
    print(f"  {ticker:8s} {n:6d} {w:5d} {wp:5.1f}% {fmt(total):>13s} {fmt(avg):>11s} {ivd_s:>9s} {ivrv_s:>9s} {ne_s:>9s}")

print(f"\n  Total tickers: {len(ticker_data)}")
print()
