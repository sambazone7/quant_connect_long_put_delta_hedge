#!/usr/bin/env python3
"""
Parse QC earnings-strategy log output into CSV.

Usage:
    python parse_log.py <logfile.txt> <output.csv>

Handles two log formats:
  - New (separate IV cols, stk_chg%, iv_min, optional MinD, iv_max, MaxD)
  - Old (arrow-style: IV entry → exit, no stk_chg% or iv_min/max)
"""
import re, sys, argparse

parser = argparse.ArgumentParser(description="Parse QC earnings log → CSV")
parser.add_argument("logfile", help="Input log file (.txt)")
parser.add_argument("output",  help="Output CSV file")
args = parser.parse_args()

# ── Regex patterns ──────────────────────────────────────────────────────────

# Ticker name from SUMMARY line
summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

# New format — separate IV columns, stk_chg%, iv_min, optional MinD, iv_max, MaxD
# Example: [+] 2022-04-28   $+27,301.00  $-25,437.94    -10.8%  $ +1,863.06     28.1%    62.0%   26.5%     27   65.4%      2    +120%    0.88
trade_new_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  put_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 5  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  combined
    r'(\d+\.?\d*)%\s+'                       # 7  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 8  iv_exit
    r'(\d+\.?\d*)%\s+'                       # 9  iv_min
    r'(?:(\d+)\s+)?'                         # 10 optional MinD
    r'(\d+\.?\d*)%\s+'                       # 11 iv_max
    r'(\d+)\s+'                              # 12 MaxD
    r'([+-]\d+)%\s+'                         # 13 iv_change
    r'(\d+\.?\d+)'                           # 14 iv_rv
)

# Old format — arrow-style IV, no stk_chg% or iv_min/max
# Example: [+] 2022-04-20 $ +5,593.00 $ -1,164.36 $ +4,428.64 23.5% → 48.6% +107% 0.86x
trade_old_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  put_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  stock_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  combined
    r'(\d+\.?\d*)%\s*→\s*(\d+\.?\d*)%\s+'   # 6,7 iv_entry → iv_exit
    r'([+-]\d+)%\s+'                         # 8  iv_change
    r'(\d+\.?\d+)x?'                         # 9  iv_rv
)

# ── Parse ───────────────────────────────────────────────────────────────────

with open(args.logfile, "r", encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

current_ticker = None
rows = []

for line in lines:
    # Check for SUMMARY line → update current ticker
    m = summary_re.search(line)
    if m:
        current_ticker = m.group(1)
        continue

    if current_ticker is None:
        continue

    # Try new format first
    m = trade_new_re.search(line)
    if m:
        rows.append({
            "ticker":              current_ticker,
            "win":                 "Win" if m.group(1) == "+" else "Loss",
            "earnings":            m.group(2),
            "put_pnl":             m.group(3).replace(",", ""),
            "stock_pnl":           m.group(4).replace(",", ""),
            "stk_chg_pct":         m.group(5),
            "combined":            m.group(6).replace(",", ""),
            "iv_entry":            m.group(7) + "%",
            "iv_exit":             m.group(8) + "%",
            "iv_min":              m.group(9) + "%",
            "iv_min_days_before":  m.group(10) if m.group(10) else "",
            "iv_max":              m.group(11) + "%",
            "iv_max_days_before":  m.group(12),
            "iv_change":           "+" + m.group(13) + "%" if not m.group(13).startswith(("+", "-")) else m.group(13) + "%",
            "iv_rv":               m.group(14),
        })
        continue

    # Fall back to old format
    m = trade_old_re.search(line)
    if m:
        rows.append({
            "ticker":              current_ticker,
            "win":                 "Win" if m.group(1) == "+" else "Loss",
            "earnings":            m.group(2),
            "put_pnl":             m.group(3).replace(",", ""),
            "stock_pnl":           m.group(4).replace(",", ""),
            "stk_chg_pct":         "",
            "combined":            m.group(5).replace(",", ""),
            "iv_entry":            m.group(6) + "%",
            "iv_exit":             m.group(7) + "%",
            "iv_min":              "",
            "iv_min_days_before":  "",
            "iv_max":              "",
            "iv_max_days_before":  "",
            "iv_change":           m.group(8) + "%",
            "iv_rv":               m.group(9),
        })
        continue

# ── Detect whether MinD column is present ───────────────────────────────────

has_mind = any(r["iv_min_days_before"] for r in rows)

# ── Write CSV ───────────────────────────────────────────────────────────────

fields = [
    "ticker", "win", "earnings", "put_pnl", "stock_pnl", "stk_chg_pct",
    "combined", "iv_entry", "iv_exit", "iv_min",
]
if has_mind:
    fields.append("iv_min_days_before")
fields.extend(["iv_max", "iv_max_days_before", "iv_change", "iv_rv"])

with open(args.output, "w", encoding="utf-8") as f:
    f.write(",".join(fields) + "\n")
    for r in rows:
        vals = [r[k] for k in fields]
        f.write(",".join(vals) + "\n")

print(f"Parsed {len(rows)} trades -> {args.output}")
