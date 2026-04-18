#!/usr/bin/env python3
"""
Parse QC calendar-spread earnings-strategy log output into CSV.

Usage:
    python cal_parse_log.py <logfile.txt> <output.csv>

Parses the current calendar put format with:
  SimPnL, VIXen/VIXex, bid-ask spread columns, hedge count.
"""
import re, sys, math, argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description="Parse QC calendar-spread log → CSV")
parser.add_argument("logfile", help="Input log file (.txt)")
parser.add_argument("output",  help="Output CSV file")
args = parser.parse_args()

# ── Regex patterns ──────────────────────────────────────────────────────────

summary_re = re.compile(r'(\w+)\s+SUMMARY\s*\|')

skip_re = re.compile(
    r'Entries attempted:\s*(\d+)\s*\|\s*Skipped:\s*(\d+)\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)

skip_totals_re = re.compile(
    r'SKIP TOTALS:\s*(\d+)\s+attempted\s*\|\s*(\d+)\s+traded\s*\|\s*(\d+)\s+skipped\s*'
    r'\(no_pair=(\d+),\s*low_debit=(\d+),\s*other=(\d+)\)'
)

# Trade row:
# [-] 2025-07-23      19  $ +3,154.00  $ -3,325.00  $ -1,566.48   +2.3%   -8.1%     -6.5%  $ -1,737.48  $ -1,290.98    19.8   17.2     62.8%    55.0%    67.5%      6.1%      0.72      +7%    0.80      285      285      285      475     12.50     8.30    15.20    10.40     19  Hdg=6
trade_re = re.compile(
    r'\[([+-])\]\s+'                          #  1  win/loss
    r'(\d{4}-\d{2}-\d{2})\s+'                #  2  earnings date
    r'(\d+)\s+'                              #  3  n_contracts
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            #  4  long_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            #  5  short_pnl
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            #  6  stock_pnl
    r'([+-]?\d+\.?\d*)%\s+'                  #  7  stk_max_up_pct
    r'([+-]?\d+\.?\d*)%\s+'                  #  8  stk_max_dn_pct
    r'([+-]?\d+\.?\d*)%\s+'                  #  9  stk_chg_pct
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 10  combined
    r'\$\s*([+-]?[\d,]+\.\d+)\s+'            # 11  sim_pnl
    r'(?:(\d+\.?\d*)|n/a)\s+'                # 12  vix_entry (float or n/a)
    r'(?:(\d+\.?\d*)|n/a)\s+'                # 13  vix_exit  (float or n/a)
    r'(\d+\.?\d*)%\s+'                       # 14  iv_long_entry
    r'(\d+\.?\d*)%\s+'                       # 15  iv_short_entry
    r'(\d+\.?\d*)%\s+'                       # 16  iv_long_exit
    r'([+-]?\d+\.?\d*)%\s+'                  # 17  ivspread
    r'(\d+\.?\d+)\s+'                        # 18  shiv_rv
    r'([+-]\d+)%\s+'                         # 19  iv_change
    r'(\d+\.?\d+)\s+'                        # 20  iv_rv
    r'(\d+)\s+'                              # 21  long_spread_entry
    r'(\d+)\s+'                              # 22  short_spread_entry
    r'(\d+)\s+'                              # 23  long_spread_exit
    r'(\d+)\s+'                              # 24  short_spread_exit
    r'(\d+\.?\d*)\s+'                        # 25  short_put_entry_px
    r'(\d+\.?\d*)\s+'                        # 26  short_put_exit_px
    r'(\d+\.?\d*)\s+'                        # 27  long_put_entry_px
    r'(\d+\.?\d*)\s+'                        # 28  long_put_exit_px
    r'(\d+)\s+'                              # 29  n_calendars
    r'Hdg=(\d+)'                             # 30  hedge_count
)

# ── Parse ───────────────────────────────────────────────────────────────────

with open(args.logfile, "r", encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

current_ticker = None
rows = []

per_ticker_skips = {}
grand_skip_line  = None

def clean(s):
    return s.replace(",", "")

for line in lines:
    m = summary_re.search(line)
    if m:
        current_ticker = m.group(1)
        continue

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

    if current_ticker is None:
        continue

    m = trade_re.search(line)
    if m:
        rows.append({
            "ticker":             current_ticker,
            "win":                "Win" if m.group(1) == "+" else "Loss",
            "earnings":           m.group(2),
            "n_contracts":        m.group(3),
            "long_pnl":           clean(m.group(4)),
            "short_pnl":          clean(m.group(5)),
            "stock_pnl":          clean(m.group(6)),
            "stk_max_up_pct":     m.group(7),
            "stk_max_dn_pct":     m.group(8),
            "stk_chg_pct":        m.group(9),
            "combined":           clean(m.group(10)),
            "sim_pnl":            clean(m.group(11)),
            "vix_entry":          m.group(12) or "",
            "vix_exit":           m.group(13) or "",
            "iv_long_entry":      m.group(14) + "%",
            "iv_short_entry":     m.group(15) + "%",
            "iv_long_exit":       m.group(16) + "%",
            "ivspread":           m.group(17) + "%",
            "shiv_rv":            m.group(18),
            "iv_change":          m.group(19) + "%",
            "iv_rv":              m.group(20),
            "long_spread_entry":  m.group(21),
            "short_spread_entry": m.group(22),
            "long_spread_exit":   m.group(23),
            "short_spread_exit":  m.group(24),
            "short_put_entry_px": m.group(25),
            "short_put_exit_px":  m.group(26),
            "long_put_entry_px":  m.group(27),
            "long_put_exit_px":   m.group(28),
            "n_calendars":        m.group(29),
            "hedge_count":        m.group(30),
        })
        continue

# ── Compute per-ticker avg SimPnL ──────────────────────────────────────────

fields = [
    "ticker", "win", "earnings", "n_contracts",
    "long_pnl", "short_pnl", "stock_pnl", "stk_max_up_pct", "stk_max_dn_pct", "stk_chg_pct",
    "combined", "sim_pnl",
    "vix_entry", "vix_exit",
    "iv_short_entry",
    "iv_long_entry", "iv_long_exit", "ivspread", "shiv_rv",
    "iv_change", "iv_rv",
    "long_spread_entry", "short_spread_entry",
    "long_spread_exit", "short_spread_exit",
    "short_put_entry_px", "short_put_exit_px",
    "long_put_entry_px", "long_put_exit_px",
    "n_calendars",
    "hedge_count", "avg_sim_pnl",
]

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
        ticker_avg[t] = str(int(math.floor(sum(vals) / len(vals))))
for r in rows:
    r["avg_sim_pnl"] = ticker_avg.get(r["ticker"], "")

# ── Write CSV ──────────────────────────────────────────────────────────────

with open(args.output, "w", encoding="utf-8") as f:
    f.write(",".join(fields) + "\n")
    for r in rows:
        vals = [r.get(k, "") for k in fields]
        f.write(",".join(vals) + "\n")

# ── Summary ────────────────────────────────────────────────────────────────

print(f"Parsed {len(rows)} calendar-spread trades -> {args.output}")

if grand_skip_line:
    g = grand_skip_line
    print(f"\nSKIP SUMMARY: {g['attempted']} entries attempted | "
          f"{g['traded']} traded | {g['skipped']} skipped")
    print(f"  no_pair (no weekly options): {g['no_pair']}")
    print(f"  low_debit (< MIN_NET_DEBIT): {g['low_debit']}")
    print(f"  other (bad price / MAX_PUT_PCT / IV filter): {g['other']}")
elif per_ticker_skips:
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
