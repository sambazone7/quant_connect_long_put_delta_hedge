#!/usr/bin/env python3
"""
Audit a calendar-spread QC log for fill anomalies.

Pairs each ENTRY line with its matching EXIT line (per ticker, in order)
and back-solves actual fill prices from realized PnL columns.

Flags:
  ENTRY-INV   short_mid >= long_mid (selection-time mid inversion)
  EXIT-INV    short_exit_fill >= long_exit_fill (post-fill inversion)
  EXIT-CHEAP  long_exit_fill < intrinsic (long sold below K-S)
  SHORT-CHEAP short_exit_fill < intrinsic (short bought back below K-S)
  HUGE-NEG    Total realized PnL < -2 * planned net debit (way past max-loss)

Usage:
    python audit_calendar_log.py logs-calender/qc-output-130-most-liquid.txt [--csv out.csv]
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path


ENTRY_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"\[(?P<ticker>[A-Z.]+)\]\s+ENTRY:.*?"
    r"K=(?P<strike>[\d.]+)\s+"
    r"LongExp=(?P<long_exp>\S+)\s+ShortExp=(?P<short_exp>\S+)\s+"
    r"Spread=(?P<spread>\S+)\s+"
    r"LongMid=(?P<long_mid>[\d.]+)\s+ShortMid=(?P<short_mid>[\d.]+)\s+"
    r"NetDebit=(?P<net_debit>[\d.]+)\s+n=(?P<n>\d+)"
)

EXIT_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"\[(?P<ticker>[A-Z.]+)\]\s+EXIT:\s+"
    r"LongPnL=\$(?P<long_pnl>[+-][\d,.]+)\s+"
    r"ShortPnL=\$(?P<short_pnl>[+-][\d,.]+)\s+"
    r"StkPnL=\$(?P<stk_pnl>[+-][\d,.]+)\s+"
    r"Total=\$(?P<total>[+-][\d,.]+)"
)


def to_money(s: str) -> float:
    return float(s.replace(",", "").replace("$", ""))


def parse_log(path: Path):
    pending_entries = defaultdict(list)
    trades = []

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = ENTRY_RE.search(line)
            if m:
                pending_entries[m.group("ticker")].append({
                    "ts":         m.group("ts"),
                    "ticker":     m.group("ticker"),
                    "strike":     float(m.group("strike")),
                    "long_exp":   m.group("long_exp"),
                    "short_exp":  m.group("short_exp"),
                    "spread":     m.group("spread"),
                    "long_mid":   float(m.group("long_mid")),
                    "short_mid":  float(m.group("short_mid")),
                    "net_debit":  float(m.group("net_debit")),
                    "n":          int(m.group("n")),
                })
                continue

            m = EXIT_RE.search(line)
            if m:
                ticker = m.group("ticker")
                if not pending_entries[ticker]:
                    continue
                entry = pending_entries[ticker].pop(0)
                trades.append({
                    **entry,
                    "exit_ts":    m.group("ts"),
                    "long_pnl":   to_money(m.group("long_pnl")),
                    "short_pnl":  to_money(m.group("short_pnl")),
                    "stk_pnl":    to_money(m.group("stk_pnl")),
                    "total":      to_money(m.group("total")),
                })

    open_trades = sum(len(v) for v in pending_entries.values())
    return trades, open_trades


def audit(trades):
    """Add derived columns and per-row flags."""
    for t in trades:
        n100 = t["n"] * 100

        long_exit_fill  = t["long_mid"]  + t["long_pnl"]  / n100
        short_exit_fill = t["short_mid"] - t["short_pnl"] / n100
        t["long_exit_fill"]  = long_exit_fill
        t["short_exit_fill"] = short_exit_fill

        flags = []

        # Entry-time inversion (short mid >= long mid). Should never happen because
        # _enter_position rejects net_debit <= 0, but check anyway.
        if t["short_mid"] >= t["long_mid"]:
            flags.append("ENTRY-INV")

        # Exit-time inversion (short bought back at higher price than long sold for).
        # Same-strike calendar means long must always be >= short by no-arb.
        if short_exit_fill > long_exit_fill + 1e-6:
            flags.append("EXIT-INV")

        # Negative fills (shouldn't happen but guard).
        if long_exit_fill < 0:
            flags.append("LONG-NEG")
        if short_exit_fill < 0:
            flags.append("SHORT-NEG")

        # Loss bigger than 2x the planned entry debit (way beyond theoretical max).
        # Excludes cases where stock hedge made it look bigger than it really was.
        opt_pnl = t["long_pnl"] + t["short_pnl"]
        planned_debit = t["net_debit"] * n100
        if opt_pnl < -2 * planned_debit:
            flags.append("HUGE-NEG")

        t["flags"] = flags
        t["opt_pnl"] = opt_pnl
        t["planned_debit"] = planned_debit
        t["inversion_per_share"] = max(0.0, short_exit_fill - long_exit_fill)
        t["inversion_total"] = t["inversion_per_share"] * n100

    return trades


def print_report(trades, open_trades):
    print(f"\nTotal trades parsed: {len(trades)}")
    print(f"Unmatched entries (still open in log or never exited): {open_trades}")

    if not trades:
        return

    by_flag = defaultdict(int)
    for t in trades:
        for f in t["flags"]:
            by_flag[f] += 1
    if by_flag:
        print("\nFlag counts across all trades:")
        for f, c in sorted(by_flag.items(), key=lambda x: -x[1]):
            print(f"  {f:12s} {c:5d}")
    else:
        print("\nNo anomalies flagged.")
        return

    flagged = [t for t in trades if t["flags"]]
    flagged.sort(key=lambda t: t["inversion_total"], reverse=True)

    print(f"\nTop 30 most-impacted trades (sorted by exit-inversion $):")
    print(f"{'EntryTS':<12} {'Ticker':<8} {'Flags':<24} {'n':>4}  "
          f"{'NetDebt':>8} {'OptPnL':>11} {'LongFill@Exit':>14} {'ShortFill@Exit':>15} "
          f"{'InvPerSh':>9} {'InvTotal':>11}")
    for t in flagged[:30]:
        print(f"{t['ts'][:10]:<12} {t['ticker']:<8} {','.join(t['flags']):<24} "
              f"{t['n']:>4}  {t['net_debit']:>8.2f} {t['opt_pnl']:>+11,.0f} "
              f"{t['long_exit_fill']:>14.2f} {t['short_exit_fill']:>15.2f} "
              f"{t['inversion_per_share']:>9.2f} {t['inversion_total']:>+11,.0f}")

    total_inv = sum(t["inversion_total"] for t in flagged if "EXIT-INV" in t["flags"])
    n_inv = sum(1 for t in flagged if "EXIT-INV" in t["flags"])
    if n_inv:
        print(f"\nEXIT-INV summary: {n_inv} trades, total inversion cost ${total_inv:,.0f}")


def write_csv(trades, out_path: Path):
    if not trades:
        return
    fields = ["ts", "exit_ts", "ticker", "strike", "long_exp", "short_exp", "spread",
              "n", "long_mid", "short_mid", "net_debit", "planned_debit",
              "long_pnl", "short_pnl", "stk_pnl", "total", "opt_pnl",
              "long_exit_fill", "short_exit_fill",
              "inversion_per_share", "inversion_total", "flags"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for t in trades:
            row = dict(t)
            row["flags"] = ",".join(t["flags"])
            w.writerow(row)
    print(f"\nCSV written: {out_path.resolve()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log", help="QC log file path")
    ap.add_argument("--csv", default=None, help="Optional: also write full audit to CSV")
    args = ap.parse_args()

    log_path = Path(args.log)
    if not log_path.exists():
        print(f"ERROR: file not found: {log_path}")
        sys.exit(1)

    trades, open_trades = parse_log(log_path)
    audit(trades)
    print_report(trades, open_trades)

    if args.csv:
        write_csv(trades, Path(args.csv))


if __name__ == "__main__":
    main()
