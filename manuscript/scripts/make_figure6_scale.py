#!/usr/bin/env python3
"""
make_figure6_scale.py: the manuscript's Figure 6 (scale): per-stay engine time on whole records vs slices (S1 sample),
wall time by number of nodes (S2 scaling), and per-stay engine time on the full data (431,231 stays). Aggregates only.

  python3 manuscript/scripts/make_figure6_scale.py [--s2 results/s2-summary-<stamp>]

Inputs: results/s1-compare-20261013T073824Z/seconds.csv; the latest s2-summary-* (summary.json).
Writes manuscript/figures/figure6.{png,svg}.
"""
import argparse
import csv
import glob
import json
import os

try:
    import matplotlib
except ImportError:
    raise SystemExit("matplotlib is needed to draw the figures: python3 -m pip install --user -r requirements-figures.txt")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RES = os.path.join(ROOT, "results")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s2")
    a = ap.parse_args()
    s2 = a.s2 or sorted(glob.glob(os.path.join(RES, "s2-summary-*")))[-1]
    s2 = s2 if os.path.isabs(s2) else os.path.join(ROOT, s2)
    summ = json.load(open(os.path.join(s2, "summary.json")))
    sec = {r["measure"][:6]: r for r in csv.DictReader(open(os.path.join(RES, "s1-compare-20261013T073824Z", "seconds.csv")))}
    full = {r["measure"][:6]: r for r in csv.DictReader(open(os.path.join(s2, "per_stay_full-run.csv")))}
    runs = {r["step"]: r for r in summ["runs"]}

    fig, axes = plt.subplots(1, 3, figsize=(12.5, 4.2))
    # A: whole record vs slice, S1 sample
    ax = axes[0]
    labels = ["median", "99th pct", "max"]
    w = 0.2
    for i, (m, col_w, col_s) in enumerate([("CMS816", "#7f7f7f", "#1f4e79"), ("CMS871", "#c0c0c0", "#6fa8dc")]):
        r = sec[m]
        whole = [float(r["ref_p50"]), float(r["ref_p99"]), float(r["ref_max"])]
        sl = [float(r["sliced_p50"]), float(r["sliced_p99"]), float(r["sliced_max"])]
        xs = [k + (i * 2 - 1.5) * w for k in range(3)]
        ax.bar(xs, whole, w, color=col_w, label=f"{m} whole record")
        ax.bar([x + w for x in xs], sl, w, color=col_s, label=f"{m} slice")
    ax.set_yscale("log")
    ax.set_xticks(range(3))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Engine seconds per stay", fontsize=8)
    ax.set_title("A  Whole record vs slice (2,000-stay sample)", loc="left", fontsize=9)
    ax.set_ylim(top=3e6)
    ax.legend(fontsize=6.5, frameon=False, loc="upper left", ncol=2)
    ax.tick_params(labelsize=7)
    # B: scaling
    ax = axes[1]
    pts = [(1, runs["scale-1"]["wall_seconds"]), (2, runs["scale-2"]["wall_seconds"]), (4, runs["scale-4"]["wall_seconds"]),
           (8, runs["cold-1"]["wall_seconds"])]
    ax.plot([p[0] for p in pts], [p[1] / 60 for p in pts], marker="o", color="#1f4e79", lw=1.4, label="measured (cold)")
    ideal = pts[0][1] / 60
    ax.plot([1, 8], [ideal, ideal / 8], ls="--", color="0.5", lw=0.9, label="linear scaling")
    for n, s in pts:
        cost = [r for r in summ["runs"] if r["nodes"] == n and r["kind"] in ("cold",)][0]["cost_nodes_usd"]
        ax.annotate(f"{s / 60:.0f} min\n${cost:.2f}", (n, s / 60), textcoords="offset points", xytext=(6, 4), fontsize=6.5)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(["1", "2", "4", "8"])
    ax.set_yticks([25, 50, 100, 200])
    ax.set_yticklabels(["25", "50", "100", "200"])
    ax.minorticks_off()
    ax.set_xlabel("Nodes (2 workers each)", fontsize=8)
    ax.set_ylabel("Wall time, minutes", fontsize=8)
    ax.set_title("B  Scaling, 2,000-stay sample", loc="left", fontsize=9)
    ax.legend(fontsize=6.5, frameon=False)
    ax.tick_params(labelsize=7)
    # C: full data, per-stay distribution
    ax = axes[2]
    q = ["p50_s", "p95_s", "p99_s", "max_s"]
    ql = ["median", "95th", "99th", "max"]
    for i, (m, col) in enumerate([("CMS816", "#1f4e79"), ("CMS871", "#6fa8dc")]):
        vals = [float(full[m][k]) for k in q]
        ax.bar([k + (i - 0.5) * 0.35 for k in range(4)], vals, 0.35, color=col,
               label=f"{m}: top 1% of stays = {100 * float(full[m]['top1pct_share']):.0f}% of time")
    ax.set_yscale("log")
    ax.set_xticks(range(4))
    ax.set_xticklabels(ql, fontsize=8)
    ax.set_ylabel("Engine seconds per stay", fontsize=8)
    fr = runs["full-run"]
    ax.set_title(f"C  All 431,231 stays: {fr['wall_seconds'] / 3600:.1f} h on 8 nodes, ${fr['cost_nodes_usd']:.2f}",
                 loc="left", fontsize=9)
    ax.legend(fontsize=6.5, frameon=False, loc="upper left")
    ax.tick_params(labelsize=7)
    for ax in axes:
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)
    fig.text(0.01, 0.005, "A: whole-record times exclude evaluations stopped at the cost cap (lower bounds). "
             "C: several stays per engine call; node cost at the on-demand price, cluster fee excluded.",
             fontsize=6.8, color="0.3")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    out = os.path.join(ROOT, "manuscript", "figures")
    os.makedirs(out, exist_ok=True)
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(out, f"figure6.{ext}"), dpi=300 if ext == "png" else None)
    print(f"from {os.path.relpath(s2, ROOT)}: wrote manuscript/figures/figure6.png, figure6.svg")


if __name__ == "__main__":
    main()
