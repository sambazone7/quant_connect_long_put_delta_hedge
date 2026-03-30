#!/usr/bin/env python3
"""
Bucket analysis of parsed trade CSV.

Usage:
    python analyze_trades.py <input.csv> <output.txt>

Runs five analyses (all using sim_pnl as the PnL metric):
  A. IV Percentile at Entry (perc_iv_en) vs sim_pnl
  B. IV/RV Ratio (iv_rv) vs sim_pnl
  C. IV Rank at Entry (ivr) vs sim_pnl
  D. VIX at Entry (vix_entry) vs sim_pnl
  E. IV at Entry Sample (iv_enter_sample) vs sim_pnl
"""
import csv, statistics, argparse


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - 1)
    sx = statistics.stdev(xs)
    sy = statistics.stdev(ys)
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)


def load_column(csv_rows, col, pnl_col="sim_pnl", strip_pct=False):
    """Extract (metric_value, sim_pnl) pairs from parsed CSV rows."""
    pairs = []
    for r in csv_rows:
        raw = r.get(col, "").strip()
        if strip_pct:
            raw = raw.rstrip("%")
        sim_raw = r.get(pnl_col, "").strip().replace(",", "").replace("+", "")
        if not raw or raw == "n/a" or not sim_raw:
            continue
        try:
            val = float(raw)
            sim = float(sim_raw)
        except Exception:
            continue
        pairs.append((val, sim))
    return pairs


def run_bucket_analysis(out, rows, bucket_defs, metric_name, corr_label):
    """Run a full bucket analysis and write results to out."""
    buckets = {name: [] for name, _ in bucket_defs}
    for val, pnl in rows:
        for name, fn in bucket_defs:
            if fn(val):
                buckets[name].append((val, pnl))
                break

    hdr = (
        f"  {'Bucket':<18} {'Trades':>6} {'Win%':>6} {'Total PnL':>14} "
        f"{'Avg PnL':>10} {'Med PnL':>10} {'Avg ' + metric_name:>10} "
        f"{'Min PnL':>10} {'Max PnL':>10} {'StdDev':>10}"
    )
    out.write(hdr + "\n")
    out.write("  " + "-" * (len(hdr) - 2) + "\n")

    for name, _ in bucket_defs:
        data = buckets[name]
        if not data:
            out.write(f"  {name:<18} {'(none)':>6}\n")
            continue
        pnls = [d[1] for d in data]
        vals = [d[0] for d in data]
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        total = sum(pnls)
        avg = total / n
        med = statistics.median(pnls)
        mn = min(pnls)
        mx = max(pnls)
        sd = statistics.stdev(pnls) if n > 1 else 0
        avg_v = sum(vals) / n
        out.write(
            f"  {name:<18} {n:>6} {wins/n*100:>5.1f}% {total:>+14,.2f} "
            f"{avg:>+10,.2f} {med:>+10,.2f} {avg_v:>9.2f} "
            f"{mn:>+10,.2f} {mx:>+10,.2f} {sd:>10,.2f}\n"
        )

    out.write("\n  Win/Loss breakdown:\n")
    hdr2 = f"  {'Bucket':<18} {'Avg Win':>10} {'Avg Loss':>10} {'W/L Ratio':>10}"
    out.write(hdr2 + "\n")
    out.write("  " + "-" * (len(hdr2) - 2) + "\n")
    for name, _ in bucket_defs:
        data = buckets[name]
        if not data:
            continue
        pnls = [d[1] for d in data]
        wins_pnl = [p for p in pnls if p > 0]
        loss_pnl = [p for p in pnls if p <= 0]
        avg_w = sum(wins_pnl) / len(wins_pnl) if wins_pnl else 0
        avg_l = sum(loss_pnl) / len(loss_pnl) if loss_pnl else 0
        ratio = abs(avg_w / avg_l) if avg_l != 0 else float("inf")
        out.write(f"  {name:<18} {avg_w:>+10,.2f} {avg_l:>+10,.2f} {ratio:>10.2f}\n")

    all_vals = [r[0] for r in rows]
    all_pnls = [r[1] for r in rows]
    r_val = pearson(all_vals, all_pnls)
    r_str = f"{r_val:.4f}" if r_val is not None else "n/a"
    out.write(f"\n  Pearson correlation ({corr_label}): r = {r_str}\n")


def main():
    parser = argparse.ArgumentParser(description="Bucket analysis of parsed trade CSV")
    parser.add_argument("input_csv", help="Input CSV file (output of parse_log.py)")
    parser.add_argument("output_txt", help="Output TXT report file")
    args = parser.parse_args()

    with open(args.input_csv, encoding="utf-8", errors="replace") as f:
        csv_rows = list(csv.DictReader(f))

    with open(args.output_txt, "w", encoding="utf-8") as out:
        out.write(f"Trade Bucket Analysis — {args.input_csv}\n")
        out.write(f"Total rows in CSV: {len(csv_rows)}\n")
        out.write("=" * 80 + "\n\n")

        # ── Section A: IV Percentile at Entry ──────────────────────────────
        rows_pctl = load_column(csv_rows, "perc_iv_en")
        out.write("=" * 80 + "\n")
        out.write("A. IV PERCENTILE AT ENTRY (perc_iv_en) vs sim_pnl\n")
        out.write(f"   Valid trades: {len(rows_pctl)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_pctl:
            pctl_buckets = [
                ("Pctl < 20",  lambda x: x < 20),
                ("Pctl 20-39", lambda x: 20 <= x < 40),
                ("Pctl 40-59", lambda x: 40 <= x < 60),
                ("Pctl 60-79", lambda x: 60 <= x < 80),
                ("Pctl 80+",   lambda x: x >= 80),
            ]
            run_bucket_analysis(out, rows_pctl, pctl_buckets, "Pctl", "perc_iv_en vs sim_pnl")
        else:
            out.write("  (no valid data for perc_iv_en)\n")

        out.write("\n\n")

        # ── Section B: IV/RV Ratio ─────────────────────────────────────────
        rows_ivrv = load_column(csv_rows, "iv_rv")
        out.write("=" * 80 + "\n")
        out.write("B. IV/RV RATIO (iv_rv) vs sim_pnl\n")
        out.write(f"   Valid trades: {len(rows_ivrv)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_ivrv:
            ivrv_buckets = [
                ("IV/RV < 0.80",   lambda x: x < 0.80),
                ("IV/RV 0.80-1.00", lambda x: 0.80 <= x < 1.00),
                ("IV/RV 1.00-1.20", lambda x: 1.00 <= x < 1.20),
                ("IV/RV 1.20-1.50", lambda x: 1.20 <= x < 1.50),
                ("IV/RV 1.50+",    lambda x: x >= 1.50),
            ]
            run_bucket_analysis(out, rows_ivrv, ivrv_buckets, "IV/RV", "iv_rv vs sim_pnl")
        else:
            out.write("  (no valid data for iv_rv)\n")

        out.write("\n\n")

        # ── Section C: IV Rank at Entry ────────────────────────────────────
        rows_ivr = load_column(csv_rows, "ivr")
        out.write("=" * 80 + "\n")
        out.write("C. IV RANK AT ENTRY (ivr) vs sim_pnl\n")
        out.write(f"   Valid trades: {len(rows_ivr)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_ivr:
            ivr_buckets = [
                ("IVR < 20",  lambda x: x < 20),
                ("IVR 20-49", lambda x: 20 <= x < 50),
                ("IVR 50-69", lambda x: 50 <= x < 70),
                ("IVR 70+",   lambda x: x >= 70),
            ]
            run_bucket_analysis(out, rows_ivr, ivr_buckets, "IVR", "ivr vs sim_pnl")
        else:
            out.write("  (no valid data for ivr)\n")

        out.write("\n\n")

        # ── Section D: VIX at Entry ───────────────────────────────────────
        rows_vix = load_column(csv_rows, "vix_entry")
        out.write("=" * 80 + "\n")
        out.write("D. VIX AT ENTRY (vix_entry) vs sim_pnl\n")
        out.write(f"   Valid trades: {len(rows_vix)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_vix:
            vix_buckets = [
                ("VIX < 15",    lambda x: x < 15),
                ("VIX 15-19.9", lambda x: 15 <= x < 20),
                ("VIX 20-24.9", lambda x: 20 <= x < 25),
                ("VIX 25-29.9", lambda x: 25 <= x < 30),
                ("VIX 30+",     lambda x: x >= 30),
            ]
            run_bucket_analysis(out, rows_vix, vix_buckets, "VIX", "vix_entry vs sim_pnl")
        else:
            out.write("  (no valid data for vix_entry)\n")

        out.write("\n\n")

        # ── Section E: IV at Entry Sample ─────────────────────────────────
        rows_ivs = load_column(csv_rows, "iv_enter_sample", strip_pct=True)
        out.write("=" * 80 + "\n")
        out.write("E. IV AT ENTRY SAMPLE (iv_enter_sample) vs sim_pnl\n")
        out.write(f"   Valid trades: {len(rows_ivs)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_ivs:
            ivs_buckets = [
                ("IVs < 20%",  lambda x: x < 20),
                ("IVs 20-30%", lambda x: 20 <= x < 30),
                ("IVs 30-40%", lambda x: 30 <= x < 40),
                ("IVs 40%+",   lambda x: x >= 40),
            ]
            run_bucket_analysis(out, rows_ivs, ivs_buckets, "IVs", "iv_enter_sample vs sim_pnl")
        else:
            out.write("  (no valid data for iv_enter_sample)\n")

        out.write("\n")


if __name__ == "__main__":
    main()
