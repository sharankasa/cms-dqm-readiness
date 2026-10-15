#!/usr/bin/env python3
"""
funnel.py: R3 step 3. Funnel plots of per-hospital completeness in eICU (registered: 95% and 99.8% control
limits around the overall proportion, Spiegelhalter 2005), from the aggregate output of
pipeline/r3/sql/10_eicu_completeness.sql. Local, aggregates only.

  python3 pipeline/r3/funnel.py --run results/r3-eicu-<stamp>

Control limits: exact binomial quantiles at the overall proportion p0 for each n, divided by n.
Writes <run>/funnel.png, funnel.svg and funnel_summary.json (hospitals outside each limit, per element).
"""
import argparse
import csv
import glob
import json
import math
import os

ELEMENTS = [  # column, label, denominator column ("eligible_stays" unless results-based)
    ("pct_glucose", "Any glucose result", "eligible_stays"),
    ("pct_bedside_glucose", "Bedside glucose", "eligible_stays"),
    ("pct_insulin", "Insulin ordered or infused", "eligible_stays"),
    ("pct_diabetes_any", "Diabetes diagnosis or history", "eligible_stays"),
    ("pct_comfort_or_eol", "Comfort-care or end-of-life note", "eligible_stays"),
    ("pct_lab_glucose", "Laboratory glucose", "eligible_stays"),
]
# The share of glucose results recorded in mg/dL is reported in funnel_summary.json, not plotted: eICU stores
# every glucose result in mg/dL (100% at every hospital), so its funnel carries no information.


def find(run, need):
    for f in sorted(glob.glob(os.path.join(run, "*.csv"))):
        head = next(csv.reader(open(f)))
        if all(c in head for c in need):
            return list(csv.DictReader(open(f)))
    raise SystemExit(f"no CSV in {run} with columns {need}")


def binom_quantile(n, p, q):
    """smallest r with P(X <= r) >= q, X ~ Binomial(n, p)"""
    if p <= 0:
        return 0
    if p >= 1:
        return n
    lp, lq = math.log(p), math.log1p(-p)
    cum = 0.0
    for r in range(n + 1):
        cum += math.exp(math.lgamma(n + 1) - math.lgamma(r + 1) - math.lgamma(n - r + 1) + r * lp + (n - r) * lq)
        if cum >= q:
            return r
    return n


def limits(n, p0, q):
    return binom_quantile(n, p0, q) / n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    a = ap.parse_args()
    overall = find(a.run, ["hospitals", "pct_glucose"])[0]
    hosp = find(a.run, ["hospital", "eligible_stays", "pct_glucose"])
    hosp = [h for h in hosp if h["hospital"] != "pooled"] + [h for h in hosp if h["hospital"] == "pooled"]

    try:
        import matplotlib
    except ImportError:
        raise SystemExit("matplotlib is needed to draw the figures: python3 -m pip install --user -r requirements-figures.txt")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 3, figsize=(11, 6.8))
    summary = {"hospitals_plotted": len(hosp), "elements": {}}
    for ax, (col, label, ncol) in zip(axes.flat, ELEMENTS):
        p0 = float(overall[col]) / 100
        pts = [(int(float(h[ncol])), float(h[col]) / 100) for h in hosp if h.get(col) not in (None, "") and h.get(ncol)]
        pts = [(n, p) for n, p in pts if n > 0]
        nmax = max(n for n, _ in pts)
        grid = sorted(set([10, 15, 20, 30, 50, 75] + [int(10 * 1.15 ** k) for k in range(60)] + [nmax]))
        grid = [n for n in grid if 10 <= n <= nmax * 1.05]
        out95 = out998 = 0
        for n, p in pts:
            lo95, hi95 = limits(n, p0, 0.025), limits(n, p0, 0.975)
            lo998, hi998 = limits(n, p0, 0.001), limits(n, p0, 0.999)
            out95 += not (lo95 <= p <= hi95)
            out998 += not (lo998 <= p <= hi998)
        for q, ls in [(0.025, "--"), (0.975, "--"), (0.001, ":"), (0.999, ":")]:
            ax.plot(grid, [100 * limits(n, p0, q) for n in grid], color="0.45", lw=0.9, ls=ls)
        ax.axhline(100 * p0, color="0.3", lw=0.8)
        ax.scatter([n for n, _ in pts], [100 * p for _, p in pts], s=9, color="black", alpha=0.75, lw=0)
        ax.set_xscale("log")
        ax.set_title(label, fontsize=9, loc="left")
        ax.set_xlabel("Glucose results per hospital" if ncol == "glucose_results" else "Eligible ICU stays per hospital", fontsize=8)
        ax.set_ylabel("%", fontsize=8)
        ax.tick_params(labelsize=7)
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)
        summary["elements"][col] = {"label": label, "overall_pct": round(100 * p0, 2), "hospitals": len(pts),
                                    "outside_95": out95, "outside_99_8": out998}
    fig.text(0.01, 0.005, "Line: overall proportion. Dashed: 95% limits. Dotted: 99.8% limits (exact binomial). "
             "Hospitals with fewer than 10 eligible stays pooled.", fontsize=7, color="0.3")
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(a.run, f"funnel.{ext}"), dpi=300 if ext == "png" else None)
    summary["glucose_results_mgdl_overall_pct"] = float(overall["pct_glucose_results_mgdl"])
    json.dump(summary, open(os.path.join(a.run, "funnel_summary.json"), "w"), indent=1)
    for col, s in summary["elements"].items():
        print(f"{s['label']:<36} overall {s['overall_pct']:6.2f}%  hospitals {s['hospitals']:4d}  "
              f"outside 95%: {s['outside_95']:3d}  outside 99.8%: {s['outside_99_8']:3d}")
    print(f"wrote {a.run}/funnel.png, funnel.svg, funnel_summary.json")


if __name__ == "__main__":
    main()
