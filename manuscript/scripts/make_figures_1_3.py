#!/usr/bin/env python3
"""
make_figures_1_3.py: draw the manuscript's Figure 1 (study design) and Figure 3 (where each readiness problem
acts in the measure logic, and in which direction). Diagrams only; no data are read. The logic follows CMS's
specifications as implemented in pipeline/v/reference_measures.py (independent implementation, header).

  python3 manuscript/scripts/make_figures_1_3.py

Writes manuscript/figures/figure1.{png,svg} and figure3.{png,svg}.
"""
import os

try:
    import matplotlib
except ImportError:
    raise SystemExit("matplotlib is needed to draw the figures: python3 -m pip install --user -r requirements-figures.txt")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FIG = os.path.join(ROOT, "manuscript", "figures")
NEW = "#dbe9f6"      # components built in this study
ENG = "#e6e6e6"      # CMS content and the unmodified engine
OUT = "#fbe3d6"      # outcomes


def box(ax, x, y, w, h, text, fc="white", ls="-", bold=False, fs=7.6):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.25,rounding_size=0.8", fc=fc, ec="black",
                                lw=0.9, ls=ls))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs, linespacing=1.3,
            fontweight="bold" if bold else "normal")


def arrow(ax, x1, y1, x2, y2, style="->"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle=style, lw=0.9, color="black"))


def figure1():
    fig, ax = plt.subplots(figsize=(11, 6.6))
    ax.set_xlim(0, 110)
    ax.set_ylim(0, 66)
    ax.axis("off")
    box(ax, 1, 50, 22, 13, "MIMIC-IV-on-FHIR 2.1\n431,231 stays\n(records as exported)", fc=ENG, bold=True)
    box(ax, 1, 31, 22, 14, "CMS816 and CMS871\nFHIR specifications\n(pinned commit), VSAC\nvalue sets, RxNorm", fc=ENG)
    box(ax, 30, 50, 22, 13, "Mapping to the measures'\ncodes and profiles\nbaseline (a) and\nvariants (b), (c), (d)", fc=NEW)
    arrow(ax, 23, 56.5, 30, 56.5)
    box(ax, 30, 31, 22, 14, "Readiness checks\nderived from the\nmeasures' requirements", fc=NEW)
    arrow(ax, 23, 38, 30, 38)
    arrow(ax, 41, 50, 41, 45)
    box(ax, 59, 52, 23, 11, "Unmodified engine\n(fqm-execution 1.8.5)\non each patient's\nwhole record", fc=ENG)
    box(ax, 59, 33, 23, 15, "Scaled pipeline\nrequirements filter →\nper-stay slice →\nparallel workers\n(same engine, unchanged)", fc=NEW)
    box(ax, 56, 18, 20, 10, "Independent\nimplementation (Python)\nfrom the written\nspecification", fc=NEW)
    arrow(ax, 52, 58, 59, 58)
    arrow(ax, 52, 55, 59, 42)
    arrow(ax, 52, 54, 56, 25)
    box(ax, 88, 46, 21, 14, "Identical results?\nstay by stay: every\npopulation and CMS871\nday count", fc=OUT, bold=True)
    arrow(ax, 82, 57.5, 88, 54)
    arrow(ax, 82, 41, 88, 50)
    box(ax, 88, 29, 21, 11, "Agreement with the\nindependent\nimplementation", fc=OUT)
    arrow(ax, 76, 23, 88, 32)
    arrow(ax, 82, 36, 88, 35)
    box(ax, 88, 13, 21, 11, "Runtime and cost\nrepeats, 1–8 nodes,\nevery stay", fc=OUT)
    arrow(ax, 82, 34, 88, 19)
    box(ax, 30, 1, 52, 13, "Effect on the measures\nre-run only the stays whose slice changes under each mapping\n"
        "variant and data scenario (status, units, links, codes, profiles,\nobservation typing); compare rates, "
        "stays entering and leaving,\nand measure components with the baseline", fc=OUT, fs=7.2)
    arrow(ax, 41, 31, 41, 14)
    arrow(ax, 79, 33, 79, 14)
    box(ax, 1, 4, 22, 14, "eICU 2.0, 208 hospitals:\ncompleteness of the\nmeasures' data elements\n(SQL only)", fc=OUT)
    for x, fc, lab in [(1, ENG, "CMS content, data and the unmodified engine"), (38, NEW, "Built in this study"),
                       (62, OUT, "Outcomes")]:
        ax.add_patch(FancyBboxPatch((x, -3.2), 2.2, 1.6, boxstyle="round,pad=0.1", fc=fc, ec="black", lw=0.6))
        ax.text(x + 3, -2.4, lab, va="center", fontsize=7.2)
    ax.set_ylim(-4, 66)
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(FIG, f"figure1.{ext}"), dpi=300 if ext == "png" else None, bbox_inches="tight")


def figure3():
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 7.4), gridspec_kw={"height_ratios": [1, 1.15]})
    # Panel A: CMS816
    a1.set_xlim(-6, 54)
    a1.set_ylim(-7.6, 10)
    a1.axis("off")
    a1.set_title("A  CMS816, severe hypoglycemia: one event per stay", loc="left", fontsize=10)
    a1.plot([0, 50], [0, 0], color="black", lw=1.2)
    a1.add_patch(plt.Rectangle((-4, -0.6), 4, 1.2, fc=ENG, ec="black", lw=0.6))
    a1.text(-2, 1.4, "ED or\nobservation\n(≤1 h before)", ha="center", fontsize=6.8)
    a1.text(0, -1.6, "admission", ha="center", fontsize=7)
    a1.text(50, -1.6, "discharge", ha="center", fontsize=7)
    a1.annotate("glucose-lowering dose\n(administration starts)", xy=(14, 0), xytext=(14, 5.2), ha="center", fontsize=7.2,
                arrowprops=dict(arrowstyle="->", lw=0.8))
    a1.annotate("glucose < 40 mg/dL\nwithin 24 h of the dose", xy=(26, 0), xytext=(26, 5.2), ha="center", fontsize=7.2,
                arrowprops=dict(arrowstyle="->", lw=0.8))
    a1.annotate("", xy=(14, -0.9), xytext=(26, -0.9), arrowprops=dict(arrowstyle="<->", lw=0.7, color="0.4"))
    a1.text(20, -2.1, "≤ 24 h", ha="center", fontsize=6.8, color="0.3")
    a1.annotate("no glucose > 80\nwithin 5 min after", xy=(27.2, 0), xytext=(36, 5.2), ha="center", fontsize=7.2,
                arrowprops=dict(arrowstyle="->", lw=0.8))
    notes816 = [
        ("Unitless or non-UCUM glucose: the < 40 comparison is null", "events lost; denominator unchanged", "−"),
        ("Not-given dose recorded as completed", "stays added to the denominator", "+"),
        ("Brand or ingredient drug code; unmapped code", "doses invisible: stays leave the denominator", "−"),
        ("Repeat glucose > 80 within 5 minutes missing", "the low result now counts as an event", "+"),
        ("Stay typed as observation", "stay outside the measure", "out"),
    ]
    for i, (p, e, d) in enumerate(notes816):
        y = -3.6 - i * 0.85
        a1.text(-5.5, y, f"[{d}]", fontsize=7.2, fontweight="bold", va="center")
        a1.text(-3.2, y, f"{p}: {e}", fontsize=7.2, va="center")
    # Panel B: CMS871
    a2.set_xlim(-6, 54)
    a2.set_ylim(-8.5, 10)
    a2.axis("off")
    a2.set_title("B  CMS871, severe hyperglycemia: event days among eligible days", loc="left", fontsize=10)
    w = 5
    for d in range(10):
        x = d * w
        fc = "#f2f2f2" if d == 0 else "white"
        a2.add_patch(plt.Rectangle((x, -0.8), w, 1.6, fc=fc, ec="black", lw=0.6))
        a2.text(x + w / 2, 0, f"day {d + 1}", ha="center", va="center", fontsize=6.8)
    a2.text(2.5, 1.5, "not\neligible", ha="center", fontsize=6.5, color="0.35")
    a2.annotate("", xy=(5, 2.6), xytext=(50, 2.6), arrowprops=dict(arrowstyle="<->", lw=0.7, color="0.4"))
    a2.text(27.5, 3.1, "eligible days: day 2 to day 10 (or discharge)", ha="center", fontsize=7, color="0.3")
    a2.text(17.5, 6.6, "event day: any glucose > 300 mg/dL", ha="center", fontsize=7.2)
    a2.annotate("", xy=(17.5, 0.9), xytext=(17.5, 6.2), arrowprops=dict(arrowstyle="->", lw=0.8))
    a2.text(37.5, 6.6, "or no glucose test, after two days\neach with a glucose ≥ 200", ha="center", fontsize=7.2)
    a2.annotate("", xy=(37.5, 0.9), xytext=(37.5, 5.9), arrowprops=dict(arrowstyle="->", lw=0.8))
    notes871 = [
        ("Unitless or non-UCUM glucose: > 300 and ≥ 200 comparisons are null", "event days lost", "−"),
        ("Glucose tests missing from the record", "a lost > 300 result removes an event day; the absence rule can create one", "±"),
        ("Encounter not linked to its diabetes diagnosis", "event-free stays leave the population: rate rises", "+"),
        ("Glucose-lowering drug unmapped or at the wrong term level", "stays leave the population", "−"),
        ("Stay typed as observation", "stay outside the measure", "out"),
    ]
    for i, (p, e, d) in enumerate(notes871):
        y = -2.6 - i * 0.95
        a2.text(-5.5, y, f"[{d}]", fontsize=7.2, fontweight="bold", va="center")
        a2.text(-3.2, y, f"{p}: {e}", fontsize=7.2, va="center")
    fig.text(0.01, 0.005, "[−] lowers the count it acts on, [+] raises it, [±] either, [out] removes the stay from the measure. "
             "Logic as specified by CMS; days are whole 24-hour periods from the start of the hospitalization.",
             fontsize=7, color="0.3")
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(FIG, f"figure3.{ext}"), dpi=300 if ext == "png" else None)


if __name__ == "__main__":
    os.makedirs(FIG, exist_ok=True)
    figure1()
    figure3()
    print("wrote manuscript/figures/figure1.{png,svg}, figure3.{png,svg}")
