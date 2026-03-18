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
from collections import defaultdict

parser = argparse.ArgumentParser(description="Parse QC earnings log → CSV")
parser.add_argument("logfile", help="Input log file (.txt)")
parser.add_argument("output",  help="Output CSV file")
args = parser.parse_args()

# ── Regex patterns ──────────────────────────────────────────────────────────

# Ticker name from SUMMARY line
summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

# Per-ticker skip counts:  "Entries attempted: 16  |  Skipped: 10 (no_pair=5, low_debit=3, other=2)"
skip_re = re.compile(
    r'Entries attempted:\s*(\d+)\s*\|\s*Skipped:\s*(\d+)\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)

# Grand total skip line:  "SKIP TOTALS: 450 attempted | 413 traded | 37 skipped (no_pair=20, low_debit=12, other=5)"
skip_totals_re = re.compile(
    r'SKIP TOTALS:\s*(\d+)\s+attempted\s*\|\s*(\d+)\s+traded\s*\|\s*(\d+)\s+skipped\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)

# ── Calendar spread: OLD format (with IV min / MinD / IV max / MaxD) ────────
# Example: [+] 2023-02-02    133  $-45,315.00  $+36,765.00  $+9,338.49  +12.5%  $+788.49  38.7%  50.6%  2.8%  1.10  37.1%  20  50.6%  7  +31%  1.19
cal_old_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'(\d+)\s+'                              # 3  n_contracts
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 7  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 8  combined
    r'(\d+\.?\d*)%\s+'                       # 9  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 10 iv_exit
    r'([+-]?\d+\.?\d*)%\s+'                   # 11 ivspread (can be negative)
    r'(\d+\.?\d+)\s+'                        # 12 shiv_rv
    r'(\d+\.?\d*)%\s+'                       # 13 iv_min
    r'(\d+)\s+'                              # 14 MinD
    r'(\d+\.?\d*)%\s+'                       # 15 iv_max
    r'(\d+)\s+'                              # 16 MaxD
    r'([+-]\d+)%\s+'                         # 17 iv_change
    r'(\d+\.?\d+)'                           # 18 iv_rv
)

# ── Calendar spread: NEW format with bid-ask spread columns ───────────────
# Example: [+] 2023-02-02    133  $-45,315.00  $+36,765.00  $+9,338.49  +12.5%  $+788.49  38.7%  50.6%  2.8%  1.10  +31%  1.19     420     310     380     290
cal_spread_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'(\d+)\s+'                              # 3  n_contracts
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 7  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 8  combined
    r'(\d+\.?\d*)%\s+'                       # 9  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 10 iv_exit
    r'([+-]?\d+\.?\d*)%\s+'                  # 11 ivspread (can be negative)
    r'(\d+\.?\d+)\s+'                        # 12 shiv_rv
    r'([+-]\d+)%\s+'                         # 13 iv_change
    r'(\d+\.?\d+)\s+'                        # 14 iv_rv
    r'(\d+)\s+'                              # 15 long_spread_entry
    r'(\d+)\s+'                              # 16 short_spread_entry
    r'(\d+)\s+'                              # 17 long_spread_exit
    r'(\d+)'                                 # 18 short_spread_exit
)

# ── Calendar spread: NEW format (no IV min/max, no spread columns) ────────
# Example: [+] 2023-02-02    133  $-45,315.00  $+36,765.00  $+9,338.49  +12.5%  $+788.49  38.7%  50.6%  2.8%  1.10  +31%  1.19
cal_new_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'(\d+)\s+'                              # 3  n_contracts
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 5  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 7  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 8  combined
    r'(\d+\.?\d*)%\s+'                       # 9  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 10 iv_exit
    r'([+-]?\d+\.?\d*)%\s+'                   # 11 ivspread (can be negative)
    r'(\d+\.?\d+)\s+'                        # 12 shiv_rv
    r'([+-]\d+)%\s+'                         # 13 iv_change
    r'(\d+\.?\d+)'                           # 14 iv_rv
)

# ── Single-put: FULL format (SimPnL + IVdif + neIVR + PSpEn/PSpEx/CSpEx) ──
# Example: [+] 2022-04-28  $+27,439.00  $-25,403.34  -10.8%  $+2,035.66  $+2,070.16  4.0%  28.2%  69.6%  27.0%  27  69.6%  1  +147%  0.88  0.76  230  460  23
sp_full_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  put_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 5  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  combined
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 7  sim_pnl
    r'(?:([+-]?\d+\.?\d*)%|n/a)\s+'          # 8  iv_diff (can be n/a)
    r'(\d+\.?\d*)%\s+'                       # 9  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 10 iv_exit
    r'(\d+\.?\d*)%\s+'                       # 11 iv_min
    r'(\d+)\s+'                              # 12 MinD
    r'(\d+\.?\d*)%\s+'                       # 13 iv_max
    r'(\d+)\s+'                              # 14 MaxD
    r'([+-]\d+)%\s+'                         # 15 iv_change
    r'(\d+\.?\d+)\s+'                        # 16 iv_rv
    r'(?:(?:(\d+\.\d+)|n/a)\s+)?'            # 17 ne_iv_rv (optional, requires decimal)
    r'(\d+)\s+'                              # 18 put_spread_entry
    r'(\d+)\s+'                              # 19 put_spread_exit
    r'(\d+)'                                 # 20 call_spread_exit
)

# ── Single-put: SimPnL format WITHOUT IVdif/neIVR (older logs with SimPnL) ──
sp_sim_re = re.compile(
    r'\[([+-])\]\s+'                          # 1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                # 2  date
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 3  put_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 4  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  # 5  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 6  combined
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 7  sim_pnl
    r'(\d+\.?\d*)%\s+'                       # 8  iv_entry
    r'(\d+\.?\d*)%\s+'                       # 9  iv_exit
    r'(\d+\.?\d*)%\s+'                       # 10 iv_min
    r'(?:(\d+)\s+)?'                         # 11 optional MinD
    r'(\d+\.?\d*)%\s+'                       # 12 iv_max
    r'(\d+)\s+'                              # 13 MaxD
    r'([+-]\d+)%\s+'                         # 14 iv_change
    r'(\d+\.?\d+)\s+'                        # 15 iv_rv
    r'(\d+)\s+'                              # 16 put_spread_entry
    r'(\d+)\s+'                              # 17 put_spread_exit
    r'(\d+)'                                 # 18 call_spread_exit
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

# ── Skip tracking ──────────────────────────────────────────────────────────
per_ticker_skips = {}          # ticker → {attempted, skipped, no_pair, low_debit, other}
grand_skip_line  = None        # captured "SKIP TOTALS" line

def clean(s):
    return s.replace(",", "")

for line in lines:
    # ── Detect ticker from SUMMARY line ──────────────────────────────────
    m = summary_re.search(line)
    if m:
        current_ticker = m.group(1)
        continue

    # ── Capture per-ticker skip counts ───────────────────────────────────
    m = skip_re.search(line)
    if m and current_ticker:
        per_ticker_skips[current_ticker] = {
            "attempted":  int(m.group(1)),
            "skipped":    int(m.group(2)),
            "no_pair":    int(m.group(3)),
            "low_debit":  int(m.group(4)),
            "other":      int(m.group(5)),
        }
        continue

    # ── Capture grand total skip line ────────────────────────────────────
    m = skip_totals_re.search(line)
    if m:
        grand_skip_line = {
            "attempted":  int(m.group(1)),
            "traded":     int(m.group(2)),
            "skipped":    int(m.group(3)),
            "no_pair":    int(m.group(4)),
            "low_debit":  int(m.group(5)),
            "other":      int(m.group(6)),
        }
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
        # Try newest format first (with bid-ask spread columns)
        m = cal_spread_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  m.group(3),
                "long_pnl":     clean(m.group(4)),
                "short_pnl":    clean(m.group(5)),
                "put_pnl":      "",
                "stock_pnl":    clean(m.group(6)),
                "stk_chg_pct":  m.group(7),
                "combined":     clean(m.group(8)),
                "iv_entry":     m.group(9) + "%",
                "iv_exit":      m.group(10) + "%",
                "ivspread":     m.group(11) + "%",
                "shiv_rv":      m.group(12),
                "iv_change":    m.group(13) + "%",
                "iv_rv":        m.group(14),
                "long_spread_entry":  m.group(15),
                "short_spread_entry": m.group(16),
                "long_spread_exit":   m.group(17),
                "short_spread_exit":  m.group(18),
                "sim_pnl": "", "iv_diff": "", "non_earn_iv_rv": "", "call_spread_exit": "",
            })
            continue

        m = cal_old_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  m.group(3),
                "long_pnl":     clean(m.group(4)),
                "short_pnl":    clean(m.group(5)),
                "put_pnl":      "",
                "stock_pnl":    clean(m.group(6)),
                "stk_chg_pct":  m.group(7),
                "combined":     clean(m.group(8)),
                "iv_entry":     m.group(9) + "%",
                "iv_exit":      m.group(10) + "%",
                "ivspread":     m.group(11) + "%",
                "shiv_rv":      m.group(12),
                "iv_change":    m.group(17) + "%",
                "iv_rv":        m.group(18),
                "long_spread_entry":  "",
                "short_spread_entry": "",
                "long_spread_exit":   "",
                "short_spread_exit":  "",
                "sim_pnl": "", "iv_diff": "", "non_earn_iv_rv": "", "call_spread_exit": "",
            })
            continue

        m = cal_new_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  m.group(3),
                "long_pnl":     clean(m.group(4)),
                "short_pnl":    clean(m.group(5)),
                "put_pnl":      "",
                "stock_pnl":    clean(m.group(6)),
                "stk_chg_pct":  m.group(7),
                "combined":     clean(m.group(8)),
                "iv_entry":     m.group(9) + "%",
                "iv_exit":      m.group(10) + "%",
                "ivspread":     m.group(11) + "%",
                "shiv_rv":      m.group(12),
                "iv_change":    m.group(13) + "%",
                "iv_rv":        m.group(14),
                "long_spread_entry":  "",
                "short_spread_entry": "",
                "long_spread_exit":   "",
                "short_spread_exit":  "",
                "sim_pnl": "", "iv_diff": "", "non_earn_iv_rv": "", "call_spread_exit": "",
            })
            continue

    # --- Single-put formats ---
    if current_format in ("singleput", None):
        # Try full format first (SimPnL + IVdif + neIVR + spreads)
        m = sp_full_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  "",
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  m.group(5),
                "combined":     clean(m.group(6)),
                "sim_pnl":      clean(m.group(7)),
                "iv_diff":      m.group(8) + "%" if m.group(8) else "",
                "iv_entry":     m.group(9) + "%",
                "iv_exit":      m.group(10) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(15) + "%",
                "iv_rv":        m.group(16),
                "non_earn_iv_rv": m.group(17) or "",
                "long_spread_entry":  m.group(18),
                "short_spread_entry": "",
                "long_spread_exit":   m.group(19),
                "short_spread_exit":  "",
                "call_spread_exit":   m.group(20),
            })
            continue

        # Try SimPnL format without IVdif/neIVR
        m = sp_sim_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  "",
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  m.group(5),
                "combined":     clean(m.group(6)),
                "sim_pnl":      clean(m.group(7)),
                "iv_diff":      "",
                "iv_entry":     m.group(8) + "%",
                "iv_exit":      m.group(9) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(14) + "%",
                "iv_rv":        m.group(15),
                "non_earn_iv_rv": "",
                "long_spread_entry":  m.group(16),
                "short_spread_entry": "",
                "long_spread_exit":   m.group(17),
                "short_spread_exit":  "",
                "call_spread_exit":   m.group(18),
            })
            continue

        # Older format (no SimPnL, no IVdif, no spreads)
        m = sp_new_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  "",
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  m.group(5),
                "combined":     clean(m.group(6)),
                "sim_pnl":      "",
                "iv_diff":      "",
                "iv_entry":     m.group(7) + "%",
                "iv_exit":      m.group(8) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(13) + "%",
                "iv_rv":        m.group(14),
                "non_earn_iv_rv": "",
                "long_spread_entry":  "",
                "short_spread_entry": "",
                "long_spread_exit":   "",
                "short_spread_exit":  "",
                "call_spread_exit":   "",
            })
            continue

        # Oldest format (arrow-style IV)
        m = sp_old_re.search(line)
        if m:
            rows.append({
                "ticker":       current_ticker,
                "win":          "Win" if m.group(1) == "+" else "Loss",
                "earnings":     m.group(2),
                "n_contracts":  "",
                "long_pnl":     "",
                "short_pnl":    "",
                "put_pnl":      clean(m.group(3)),
                "stock_pnl":    clean(m.group(4)),
                "stk_chg_pct":  "",
                "combined":     clean(m.group(5)),
                "sim_pnl":      "",
                "iv_diff":      "",
                "iv_entry":     m.group(6) + "%",
                "iv_exit":      m.group(7) + "%",
                "ivspread":     "",
                "shiv_rv":      "",
                "iv_change":    m.group(8) + "%",
                "iv_rv":        m.group(9),
                "non_earn_iv_rv": "",
                "long_spread_entry":  "",
                "short_spread_entry": "",
                "long_spread_exit":   "",
                "short_spread_exit":  "",
                "call_spread_exit":   "",
            })
            continue

# ── Write CSV (fixed superset of all columns) ────────────────────────────

fields = [
    "ticker", "win", "earnings", "n_contracts",
    "long_pnl", "short_pnl", "put_pnl", "stock_pnl",
    "stk_chg_pct", "combined", "sim_pnl",
    "iv_diff", "non_earn_iv_rv",
    "iv_entry", "iv_exit",
    "ivspread", "shiv_rv",
    "iv_change", "iv_rv",
    "long_spread_entry", "short_spread_entry",
    "long_spread_exit", "short_spread_exit",
    "call_spread_exit",
    "avg_sim_pnl",
]

# Compute per-ticker avg SimPnL (rounded down to nearest dollar)
ticker_sim = defaultdict(list)
for r in rows:
    v = r.get("sim_pnl", "")
    if v:
        try:
            ticker_sim[r["ticker"]].append(float(v))
        except ValueError:
            pass
ticker_avg = {}
for t, vals in ticker_sim.items():
    if vals:
        import math
        ticker_avg[t] = str(int(math.floor(sum(vals) / len(vals))))
for r in rows:
    r["avg_sim_pnl"] = ticker_avg.get(r["ticker"], "")

with open(args.output, "w", encoding="utf-8") as f:
    f.write(",".join(fields) + "\n")
    for r in rows:
        vals = [r.get(k, "") for k in fields]
        f.write(",".join(vals) + "\n")

# ── Summary ──────────────────────────────────────────────────────────────
n_cal = sum(1 for r in rows if r["long_pnl"])
n_sp  = sum(1 for r in rows if r["put_pnl"])
print(f"Parsed {len(rows)} trades ({n_cal} calendar, {n_sp} single-put) -> {args.output}")

# ── Skip summary (if found in log) ──────────────────────────────────────
if grand_skip_line:
    g = grand_skip_line
    print(f"\nSKIP SUMMARY: {g['attempted']} entries attempted | "
          f"{g['traded']} traded | {g['skipped']} skipped")
    print(f"  no_pair (no weekly options): {g['no_pair']}")
    print(f"  low_debit (< MIN_NET_DEBIT): {g['low_debit']}")
    print(f"  other (bad price / MAX_PUT_PCT / IV filter): {g['other']}")
elif per_ticker_skips:
    # Aggregate from per-ticker data if grand totals not present
    tot_att = sum(v["attempted"] for v in per_ticker_skips.values())
    tot_sk  = sum(v["skipped"]   for v in per_ticker_skips.values())
    tot_np  = sum(v["no_pair"]   for v in per_ticker_skips.values())
    tot_ld  = sum(v["low_debit"] for v in per_ticker_skips.values())
    tot_ot  = sum(v["other"]     for v in per_ticker_skips.values())
    print(f"\nSKIP SUMMARY: {tot_att} entries attempted | "
          f"{tot_att - tot_sk} traded | {tot_sk} skipped")
    print(f"  no_pair (no weekly options): {tot_np}")
    print(f"  low_debit (< MIN_NET_DEBIT): {tot_ld}")
    print(f"  other (bad price / MAX_PUT_PCT / IV filter): {tot_ot}")
