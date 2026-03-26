#!/usr/bin/env python3
"""
Parse QC short-put strategy log output into CSV.

Usage:
    python parse_short_put_log.py <logfile.txt> <output.csv>

Parses trade lines of the form:
  [-] 2022-03-25  2022-04-14  days=20  PutPnL=$-3,842  StkPnL=$+2,397
      Stk=-4.1%  PnL=$-1,445  IVR=22 IV/RV=0.78  IVen=24.9% IVex=23.4%
      TrMin=23.9% TrMax=27.1%  HMin=18.7% HMax=40.5%  SpEn=102 SpEx=340
"""
import re, sys, argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description="Parse QC short-put log → CSV")
parser.add_argument("logfile", help="Input log file (.txt)")
parser.add_argument("output",  help="Output CSV file")
args = parser.parse_args()

summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

trade_re = re.compile(
    r'\[([+-])\]\s+'
    r'(\d{4}-\d{2}-\d{2})\s+'
    r'(\d{4}-\d{2}-\d{2})\s+'
    r'days=(\d+)\s+'
    r'PutPnL=\$([+-]?[\d,]+)\s+'
    r'StkPnL=\$([+-]?[\d,]+)\s+'
    r'Stk=([+-]?\d+\.?\d*)%\s+'
    r'PnL=\$([+-]?[\d,]+)\s+'
    r'IVR=(\d+)\s+'
    r'IV/RV=(\d+\.?\d*)\s+'
    r'IVen=(\d+\.?\d*)%\s+'
    r'IVex=(\d+\.?\d*)%\s+'
    r'TrMin=(\d+\.?\d*)%\s+'
    r'TrMax=(\d+\.?\d*)%\s+'
    r'HMin=(\d+\.?\d*)%\s+'
    r'HMax=(\d+\.?\d*)%\s+'
    r'SpEn=(\d+)\s+'
    r'SpEx=(\d+)'
)

with open(args.logfile, "r", encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

current_ticker = None
rows = []

def clean(s):
    return s.replace(",", "")

for line in lines:
    m = summary_re.search(line)
    if m:
        current_ticker = m.group(1)
        continue

    if current_ticker is None:
        continue

    m = trade_re.search(line)
    if m:
        rows.append({
            "ticker":        current_ticker,
            "win":           "Win" if m.group(1) == "+" else "Loss",
            "entry_date":    m.group(2),
            "exit_date":     m.group(3),
            "days":          m.group(4),
            "put_pnl":       clean(m.group(5)),
            "stk_pnl":       clean(m.group(6)),
            "stk_chg_pct":   m.group(7),
            "total_pnl":     clean(m.group(8)),
            "ivr":           m.group(9),
            "iv_rv":         m.group(10),
            "iv_entry":      m.group(11),
            "iv_exit":       m.group(12),
            "iv_trade_min":  m.group(13),
            "iv_trade_max":  m.group(14),
            "iv_hist_min":   m.group(15),
            "iv_hist_max":   m.group(16),
            "spread_entry":  m.group(17),
            "spread_exit":   m.group(18),
        })

fields = [
    "ticker", "win", "entry_date", "exit_date", "days",
    "put_pnl", "stk_pnl", "stk_chg_pct", "total_pnl",
    "ivr", "iv_rv", "iv_entry", "iv_exit",
    "iv_trade_min", "iv_trade_max", "iv_hist_min", "iv_hist_max",
    "spread_entry", "spread_exit",
]

with open(args.output, "w", encoding="utf-8") as f:
    f.write(",".join(fields) + "\n")
    for r in rows:
        f.write(",".join(r[k] for k in fields) + "\n")

ticker_counts = defaultdict(lambda: {"total": 0, "wins": 0})
for r in rows:
    ticker_counts[r["ticker"]]["total"] += 1
    if r["win"] == "Win":
        ticker_counts[r["ticker"]]["wins"] += 1

total_wins = sum(v["wins"] for v in ticker_counts.values())
print(f"Parsed {len(rows)} trades -> {args.output}")
print()
for t in ticker_counts:
    c = ticker_counts[t]
    wr = c["wins"] / c["total"] * 100 if c["total"] else 0
    print(f"  {t:<6}  {c['total']:>3} trades  {c['wins']:>3} wins  ({wr:.1f}%)")
print()
wr = total_wins / len(rows) * 100 if rows else 0
print(f"  TOTAL   {len(rows):>3} trades  {total_wins:>3} wins  ({wr:.1f}%)")
