#!/usr/bin/env python3
"""
Bucket analysis of parsed trade CSV.

Usage:
    python analyze_trades.py <input.csv> <output.txt>

Report sections (all using sim_pnl as the PnL metric):
  TOP 100 BEST & WORST TRADES (sorted by sim_pnl)
  A. IV Percentile at Entry (perc_iv_en) vs sim_pnl
  B. IV/RV Ratio (iv_rv) vs sim_pnl
  C. IV Rank at Entry (ivr) vs sim_pnl
  D. VIX at Entry (vix_entry) vs sim_pnl
  E. IV at Entry Sample (iv_enter_sample) vs sim_pnl
  F. IV at Entry (iv_entry) vs sim_pnl  [quartile buckets]
  G. Per-Ticker IV % Increase (iv_entry -> iv_exit)
  H. Monthly PnL (filtered: IV/RV < 1.5 and VIX < 24)
"""
import csv, statistics, argparse, re
from collections import defaultdict


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


def _money(s):
    """Parse a money string, returning float or None on failure."""
    if s is None:
        return None
    s = s.strip().replace(",", "").replace("+", "").replace("$", "")
    if not s or s == "n/a":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _pct(s):
    """Parse a percent string (with or without trailing %), returning float or None."""
    if s is None:
        return None
    s = s.strip().rstrip("%").replace("+", "")
    if not s or s == "n/a":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _num(s):
    """Parse a plain numeric string, returning float or None on failure."""
    if s is None:
        return None
    s = s.strip().replace(",", "")
    if not s or s == "n/a":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _int(s):
    """Parse an integer string, returning int or 0 on failure."""
    if s is None:
        return 0
    s = s.strip().replace(",", "")
    if not s or s == "n/a":
        return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def build_typed_rows(csv_rows):
    """Build a list of typed dicts from raw csv_rows. Skips rows missing sim_pnl."""
    out_rows = []
    for r in csv_rows:
        sim_pnl = _money(r.get("sim_pnl", ""))
        if sim_pnl is None:
            continue
        out_rows.append({
            "ticker":           r.get("ticker", "").strip(),
            "earnings":         r.get("earnings", "").strip(),
            "n_contracts":      _int(r.get("n_contracts", "")),
            "put_pnl":          _money(r.get("put_pnl", "")),
            "long_pnl":         _money(r.get("long_pnl", "")),
            "short_pnl":        _money(r.get("short_pnl", "")),
            "stock_pnl":        _money(r.get("stock_pnl", "")),
            "combined":         _money(r.get("combined", "")),
            "sim_pnl":          sim_pnl,
            "stk_chg_pct":      _pct(r.get("stk_chg_pct", "")),
            "iv_entry":         _pct(r.get("iv_entry", "")),
            "iv_exit":          _pct(r.get("iv_exit", "")),
            "iv_enter_sample":  _pct(r.get("iv_enter_sample", "")),
            "iv_exit_sample":   _pct(r.get("iv_exit_sample", "")),
            "perc_iv_en":       _pct(r.get("perc_iv_en", "")),
            "perc_iv_ex":       _pct(r.get("perc_iv_ex", "")),
            "ivr":              _pct(r.get("ivr", "")),
            "ivrex":            _pct(r.get("ivrex", "")),
            "vix_entry":        _num(r.get("vix_entry", "")),
            "vix_exit":         _num(r.get("vix_exit", "")),
            "iv_rv":            _num(r.get("iv_rv", "")),
            "shiv_rv":          _num(r.get("shiv_rv", "")),
            "ivspread":         _num(r.get("ivspread", "")),
            "iv_change":        _num(r.get("iv_change", "")),
        })
    return out_rows


def _fmt(v, fmt_str, suffix="", na="n/a"):
    """Format a value, returning 'n/a' if None."""
    if v is None:
        return na
    return f"{v:{fmt_str}}{suffix}"


def _fmt_money(v, width=11):
    """Format a money value as ' $+1,234' (right-aligned), or n/a centered."""
    if v is None:
        return f"{'n/a':>{width + 1}}"
    return f"${v:>+{width},.0f}"


def write_top_table(out, label, trade_list):
    """Write a top-N best/worst trades table to `out`."""
    out.write(f"\n{label}:\n")
    hdr = (f"  {'Ticker':<7} {'Earnings':>10} {'n':>5}"
           f"  {'Put PnL':>12} {'Stock PnL':>12} {'StkChg%':>8}"
           f" {'Combined':>12} {'SimPnL':>12}"
           f"  {'IVent':>7} {'IVexit':>7}"
           f" {'VIXen':>7} {'VIXex':>7}"
           f" {'IVR':>6} {'IV/RV':>6} {'Pctl':>5}\n")
    out.write(hdr)
    out.write("  " + "-" * (len(hdr) - 3) + "\n")
    for r in trade_list:
        put_pnl = r["put_pnl"] if r["put_pnl"] is not None else r["long_pnl"]
        out.write(
            f"  {r['ticker']:<7} {r['earnings']:>10} {r['n_contracts']:>5}"
            f"  {_fmt_money(put_pnl):>12} {_fmt_money(r['stock_pnl']):>12}"
            f" {_fmt(r['stk_chg_pct'], '+.1f', '%'):>8}"
            f" {_fmt_money(r['combined']):>12} {_fmt_money(r['sim_pnl']):>12}"
            f"  {_fmt(r['iv_entry'], '.1f', '%'):>7}"
            f" {_fmt(r['iv_exit'], '.1f', '%'):>7}"
            f" {_fmt(r['vix_entry'], '.1f'):>7}"
            f" {_fmt(r['vix_exit'], '.1f'):>7}"
            f" {_fmt(r['ivr'], '.1f'):>6}"
            f" {_fmt(r['iv_rv'], '.2f'):>6}"
            f" {_fmt(r['perc_iv_en'], '.0f'):>5}"
            f"\n"
        )


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

    rows = build_typed_rows(csv_rows)

    with open(args.output_txt, "w", encoding="utf-8") as out:
        out.write(f"Trade Bucket Analysis — {args.input_csv}\n")
        out.write(f"Total rows in CSV: {len(csv_rows)}\n")
        out.write("=" * 80 + "\n\n")

        # ── TOP 100 BEST & WORST TRADES ─────────────────────────────────────
        out.write("=" * 80 + "\n")
        out.write("TOP 100 BEST & WORST TRADES (sorted by sim_pnl)\n")
        out.write(f"   Valid trades: {len(rows)}\n")
        out.write("=" * 80 + "\n")

        sorted_by_pnl = sorted(rows, key=lambda r: r["sim_pnl"])
        write_top_table(out, "TOP 100 WORST TRADES", sorted_by_pnl[:100])
        write_top_table(out, "TOP 100 BEST TRADES",  sorted_by_pnl[-100:][::-1])
        out.write("\n\n")

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

        out.write("\n\n")

        # ── Section F: IV at Entry (quartile buckets) ────────────────────────
        rows_iven = load_column(csv_rows, "iv_entry", strip_pct=True)
        out.write("=" * 80 + "\n")
        out.write("F. IV AT ENTRY (iv_entry) vs sim_pnl  [quartile buckets]\n")
        out.write(f"   Valid trades: {len(rows_iven)}\n")
        out.write("=" * 80 + "\n\n")

        if rows_iven:
            vals_sorted = sorted(v for v, _ in rows_iven)
            q1, q2, q3 = statistics.quantiles(vals_sorted, n=4)
            out.write(f"  Quartile breakpoints: Q1={q1:.1f}%  Q2={q2:.1f}%  Q3={q3:.1f}%\n\n")

            iven_buckets = [
                (f"IVen < {q1:.1f}%",        lambda x, _q1=q1: x < _q1),
                (f"IVen {q1:.1f}-{q2:.1f}%", lambda x, _q1=q1, _q2=q2: _q1 <= x < _q2),
                (f"IVen {q2:.1f}-{q3:.1f}%", lambda x, _q2=q2, _q3=q3: _q2 <= x < _q3),
                (f"IVen >= {q3:.1f}%",        lambda x, _q3=q3: x >= _q3),
            ]
            run_bucket_analysis(out, rows_iven, iven_buckets, "IVen", "iv_entry vs sim_pnl")
        else:
            out.write("  (no valid data for iv_entry)\n")

        out.write("\n\n")

        # ── Section G: Per-Ticker IV % Increase (iv_entry -> iv_exit) ────────
        out.write("=" * 80 + "\n")
        out.write("G. PER-TICKER IV % INCREASE  (iv_exit - iv_entry) / iv_entry\n")
        out.write("=" * 80 + "\n\n")

        ticker_pcts = defaultdict(list)
        ticker_pnls = defaultdict(list)
        ticker_ivrv = defaultdict(list)
        skipped = 0
        for r in csv_rows:
            tk = r.get("ticker", "").strip()
            iv_en_raw = r.get("iv_entry", "").strip().rstrip("%")
            iv_ex_raw = r.get("iv_exit", "").strip().rstrip("%")
            sim_raw = r.get("sim_pnl", "").strip().replace(",", "").replace("+", "")
            ivrv_raw = r.get("iv_rv", "").strip()
            if not iv_en_raw or not iv_ex_raw or not tk:
                skipped += 1
                continue
            try:
                iv_en = float(iv_en_raw)
                iv_ex = float(iv_ex_raw)
                sim = float(sim_raw) if sim_raw else 0.0
            except Exception:
                skipped += 1
                continue
            if iv_en == 0:
                skipped += 1
                continue
            ticker_pcts[tk].append((iv_ex - iv_en) / iv_en * 100)
            ticker_pnls[tk].append(sim)
            try:
                if ivrv_raw and ivrv_raw != "n/a":
                    ticker_ivrv[tk].append(float(ivrv_raw))
            except Exception:
                pass

        all_pcts = [p for vals in ticker_pcts.values() for p in vals]
        all_pnls_g = [p for vals in ticker_pnls.values() for p in vals]
        all_ivrv = [v for vals in ticker_ivrv.values() for v in vals]
        out.write(f"  Valid trades: {len(all_pcts)}   Skipped: {skipped}\n")
        out.write(f"  Unique tickers: {len(ticker_pcts)}\n\n")

        hdr_g = f"  {'Ticker':<8} {'Trades':>6} {'Avg IV % Increase':>18} {'Median':>10} {'Avg PnL':>12} {'Avg IV/RV':>10}"
        out.write(hdr_g + "\n")
        out.write("  " + "-" * (len(hdr_g) - 2) + "\n")

        tickers_by_iv = sorted(ticker_pcts, key=lambda t: sum(ticker_pcts[t]) / len(ticker_pcts[t]), reverse=True)
        for tk in tickers_by_iv:
            vals = ticker_pcts[tk]
            pnls_tk = ticker_pnls[tk]
            ivrv_tk = ticker_ivrv.get(tk, [])
            avg = sum(vals) / len(vals)
            med = statistics.median(vals)
            avg_pnl = sum(pnls_tk) / len(pnls_tk)
            avg_ivrv = sum(ivrv_tk) / len(ivrv_tk) if ivrv_tk else 0
            ivrv_str = f"{avg_ivrv:>10.2f}" if ivrv_tk else f"{'n/a':>10}"
            out.write(f"  {tk:<8} {len(vals):>6} {avg:>+17.1f}% {med:>+9.1f}% {avg_pnl:>+12,.2f} {ivrv_str}\n")

        if all_pcts:
            overall_avg = sum(all_pcts) / len(all_pcts)
            overall_med = statistics.median(all_pcts)
            overall_pnl = sum(all_pnls_g) / len(all_pnls_g)
            overall_ivrv = sum(all_ivrv) / len(all_ivrv) if all_ivrv else 0
            out.write("  " + "-" * (len(hdr_g) - 2) + "\n")
            out.write(f"  {'OVERALL':<8} {len(all_pcts):>6} {overall_avg:>+17.1f}% {overall_med:>+9.1f}% {overall_pnl:>+12,.2f} {overall_ivrv:>10.2f}\n")
            avg_of_avgs = sum(sum(v) / len(v) for v in ticker_pcts.values()) / len(ticker_pcts)
            out.write(f"\n  Avg of per-ticker avgs (equal-weight): {avg_of_avgs:+.1f}%\n")

        out.write("\n")

        # ── Section H: Monthly PnL (IV/RV < 1.5 and VIX < 24) ─────────────
        out.write("=" * 80 + "\n")
        out.write("H. MONTHLY PnL  (filtered: IV/RV < 1.5 AND VIX at entry < 24)\n")
        out.write("=" * 80 + "\n\n")

        MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        monthly_data = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        monthly_skipped = 0

        for r in csv_rows:
            try:
                ivrv = float(r.get("iv_rv", "").strip())
                vix = float(r.get("vix_entry", "").strip())
                pnl = float(r.get("sim_pnl", "").strip().replace(",", "").replace("+", ""))
                earnings_str = r.get("earnings", "").strip()
                ym_match = re.match(r"(\d{4})-(\d{2})", earnings_str)
                if not ym_match:
                    monthly_skipped += 1
                    continue
                if ivrv < 1.5 and vix < 24:
                    ym_key = ym_match.group(0)
                    monthly_data[ym_key]["pnl"] += pnl
                    monthly_data[ym_key]["trades"] += 1
                    if r.get("win", "").strip() == "Win":
                        monthly_data[ym_key]["wins"] += 1
            except Exception:
                monthly_skipped += 1

        if monthly_data:
            all_yms = sorted(monthly_data.keys())
            first_year = int(all_yms[0][:4])
            last_year = int(all_yms[-1][:4])

            hdr_h = f"  {'Month':<12} {'Trades':>6} {'Wins':>5} {'Win%':>6} {'Avg PnL':>10} {'Total PnL':>12} {'Cumulative':>12}"
            out.write(hdr_h + "\n")
            out.write("  " + "-" * (len(hdr_h) - 2) + "\n")

            cumulative = 0.0
            total_trades_h = 0
            total_wins_h = 0
            total_pnl_h = 0.0

            for year in range(first_year, last_year + 1):
                for month in range(1, 13):
                    ym = f"{year}-{month:02d}"
                    label = f"{year} {MONTH_NAMES[month - 1]}"
                    d = monthly_data.get(ym)
                    if d and d["trades"] > 0:
                        cumulative += d["pnl"]
                        total_trades_h += d["trades"]
                        total_wins_h += d["wins"]
                        total_pnl_h += d["pnl"]
                        win_pct = d["wins"] / d["trades"] * 100
                        avg_pnl = d["pnl"] / d["trades"]
                        out.write(
                            f"  {label:<12} {d['trades']:>6} {d['wins']:>5} "
                            f"{win_pct:>5.1f}% {avg_pnl:>+10,.0f} "
                            f"{d['pnl']:>+12,.0f} {cumulative:>+12,.0f}\n"
                        )
                    else:
                        out.write(f"  {label:<12} {'0':>6}\n")

            out.write("  " + "-" * (len(hdr_h) - 2) + "\n")
            if total_trades_h > 0:
                overall_win_pct = total_wins_h / total_trades_h * 100
                overall_avg = total_pnl_h / total_trades_h
                out.write(
                    f"  {'TOTAL':<12} {total_trades_h:>6} {total_wins_h:>5} "
                    f"{overall_win_pct:>5.1f}% {overall_avg:>+10,.0f} "
                    f"{total_pnl_h:>+12,.0f} {cumulative:>+12,.0f}\n"
                )
        else:
            out.write("  (no valid data for monthly PnL — need iv_rv, vix_entry, sim_pnl, earnings columns)\n")

        out.write("\n")


if __name__ == "__main__":
    main()
