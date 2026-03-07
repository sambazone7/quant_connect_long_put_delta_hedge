#!/usr/bin/env python3
"""
Parse QC earnings-strategy log output into CSV.

Usage:
    python parse_log.py <logfile.txt> <output.csv>

Auto-detects log format:
  - Single-put strategy  (Put PnL + Stock PnL, with optional IV min/max)
  - Calendar spread       (Long PnL + Short PnL + Stock PnL, IVspread, ShIV/RV)
  - Old arrow-style       (IV entry → exit, no stk_chg% or iv_min/max)
"""
import re, sys, argparse

parser = argparse.ArgumentParser(description="Parse QC earnings log → CSV")
parser.add_argument("logfile", help="Input log file (.txt)")
parser.add_argument("output",  help="Output CSV file")
args = parser.parse_args()

# ── Regex patterns ──────────────────────────────────────────────────────────

# Ticker name from SUMMARY line
summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

# ── Calendar spread: OLD format (with IV min / MinD / IV max / MaxD) ────────
# Example: [+] 2023-02-02  $-45,315.00  $+36,765.00  $+9,338.49  +12.5%  $+788.49  38.7%  50.6%  2.8%  1.10  37.1%  20  50.6%  7  +31%  1.19
cal_old_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 6  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 7  combined
    r'(\d+\.?\d*)%\s+'                       # 8  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 9  iv_exit
    r'([+-]?\d+\.?\d*)%\s+'                   # 10 ivspread (can be negative)
    r'(\d+\.?\d+)\s+'                        # 11 shiv_rv
    r'(\d+\.?\d*)%\s+'                       # 12 iv_min
    r'(\d+)\s+'                              # 13 MinD
    r'(\d+\.?\d*)%\s+'                       # 14 iv_max
    r'(\d+)\s+'                              # 15 MaxD
    r'([+-]\d+)%\s+'                         # 16 iv_change
    r'(\d+\.?\d+)'                           # 17 iv_rv
)

# ── Calendar spread: NEW format (no IV min/max) ────────────────────────────
# Example: [+] 2023-02-02  $-45,315.00  $+36,765.00  $+9,338.49  +12.5%  $+788.49  38.7%  50.6%  2.8%  1.10  +31%  1.19
cal_new_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 6  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 7  combined
    r'(\d+\.?\d*)%\s+'                       # 8  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 9  iv_exit
    r'([+-]?\d+\.?\d*)%\s+'                   # 10 ivspread (can be negative)
    r'(\d+\.?\d+)\s+'                        # 11 shiv_rv
    r'([+-]\d+)%\s+'                         # 12 iv_change
    r'(\d+\.?\d+)'                           # 13 iv_rv
)

# ── Single-put: NEW format (separate IV cols, stk_chg%, iv_min, iv_max) ────
# Example: [+] 2022-04-28  $+27,301.00  $-25,437.94  -10.8%  $+1,863.06  28.1%  62.0%  26.5%  27  65.4%  2  +120%  0.88
sp_new_re = re.compile(
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

# ── Single-put: OLD format (arrow-style IV, no stk_chg%) ──────────────────
# Example: [+] 2022-04-20  $+5,593.00  $-1,164.36  $+4,428.64  23.5% → 48.6%  +107%  0.86
sp_old_re = re.compile(
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
current_format = None          # "calendar" | "singleput" | None
rows = []

def clean(s):
    return s.replace(",", "")

for line in lines:
    # ── Detect ticker from SUMMARY line ──────────────────────────────────
    m = summary_re.search(line)
    if m:
        current_ticker = m.group(1)
        continue

    # ── Detect format from header line ───────────────────────────────────
    if "Long PnL" in line and "Short PnL" in line:
        current_format = "calendar"
        continue
    if "Put PnL" in line and "Stock PnL" in line:
        current_format = "singleput"
        continue

    if current_ticker is None:
        continue

    # ── Try regex patterns based on detected format ──────────────────────

    # --- Calendar spread formats ---
    if current_format in ("calendar", None):
        m = cal_old_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "long_pnl":     clean(m.group(3)),
                "short_pnl":    clean(m.group(4)),
                "put_pnl":      "",
                "stock_pnl":    clean(m.group(5)),
                "stk_chg_pct":  m.group(6),
                "combined":     clean(m.group(7)),
                "iv_entry":     m.group(8) + "%",
                "iv_exit":      m.group(9) + "%",
                "ivspread":     m.group(10) + "%",
                "shiv_rv":      m.group(11),
                "iv_change":    m.group(16) + "%",
                "iv_rv":        m.group(17),
            })
            continue

        m = cal_new_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "long_pnl":     clean(m.group(3)),
                "short_pnl":    clean(m.group(4)),
                "put_pnl":      "",
                "stock_pnl":    clean(m.group(5)),
                "stk_chg_pct":  m.group(6),
                "combined":     clean(m.group(7)),
                "iv_entry":     m.group(8) + "%",
                "iv_exit":      m.group(9) + "%",
                "ivspread":     m.group(10) + "%",
                "shiv_rv":      m.group(11),
                "iv_change":    m.group(12) + "%",
                "iv_rv":        m.group(13),
            })
            continue

    # --- Single-put formats ---
    if current_format in ("singleput", None):
        m = sp_new_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  m.group(5),
                "combined":     clean(m.group(6)),
                "iv_entry":     m.group(7) + "%",
                "iv_exit":      m.group(8) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(13) + "%",
                "iv_rv":        m.group(14),
            })
            continue

        m = sp_old_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  "",
                "combined":     clean(m.group(5)),
                "iv_entry":     m.group(6) + "%",
                "iv_exit":      m.group(7) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(8) + "%",
                "iv_rv":        m.group(9),
            })
            continue

# ── Write CSV (fixed superset of all columns) ────────────────────────────

fields = [
    "ticker", "win", "earnings",
    "long_pnl", "short_pnl", "put_pnl", "stock_pnl",
    "stk_chg_pct", "combined",
    "iv_entry", "iv_exit",
    "ivspread", "shiv_rv",
    "iv_change", "iv_rv",
]

with open(args.output, "w", encoding="utf-8") as f:
    f.write(",".join(fields) + "\n")
    for r in rows:
        vals = [r.get(k, "") for k in fields]
        f.write(",".join(vals) + "\n")

# ── Summary ──────────────────────────────────────────────────────────────
n_cal = sum(1 for r in rows if r["long_pnl"])
n_sp  = sum(1 for r in rows if r["put_pnl"])
print(f"Parsed {len(rows)} trades ({n_cal} calendar, {n_sp} single-put) -> {args.output}")
