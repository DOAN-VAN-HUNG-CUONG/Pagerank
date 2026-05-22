"""
Generate the comparative-study figures from real measured data.

Five IEEE-style figures (vector PDF, single-column width) are written to
``config.FIGURES_DIR``:

    1. runtime_scaling.pdf  — wall-clock time vs graph size (log-log), per framework
    2. memory_scaling.pdf   — peak resident memory vs graph size, per framework
    3. speedup.pdf          — speedup relative to the Core (Python) baseline
    4. accuracy.pdf         — max per-node |Δ| vs the reference engine (log scale)
    5. convergence.pdf      — L1 delta per iteration (reference engine)

Figures 1-4 are built from ``config.RESULTS_CSV`` (produced by benchmark.py);
figure 5 is computed directly from the reference engine on ``--input``.

Usage:
    python tools/benchmark.py --generate           # produce the CSV first
    python tools/visualize.py                       # then the figures
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: E402

sys.path.insert(0, os.path.join(config.PROJECT_ROOT, "python"))
from core import pagerank, read_edges, top_n  # noqa: E402

plt.rcParams.update({
    "font.size": 9, "axes.titlesize": 9, "axes.labelsize": 9,
    "legend.fontsize": 7, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "savefig.bbox": "tight", "axes.grid": True, "grid.alpha": 0.3,
})
FIGSIZE = (3.5, 2.6)   # IEEE single-column width


def _load_csv(path):
    """Group OK rows by framework label -> sorted list of dict rows."""
    by_fw = defaultdict(list)
    if not os.path.isfile(path):
        return by_fw
    with open(path, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("status") != "ok":
                continue
            try:
                row["n_nodes"] = int(row["n_nodes"])
                row["time_s"] = float(row["time_s"])
                row["peak_mem_mb"] = float(row["peak_mem_mb"]) if row["peak_mem_mb"] else None
                row["max_diff_vs_core"] = (float(row["max_diff_vs_core"])
                                           if row["max_diff_vs_core"] not in ("", "0") else 0.0)
            except (ValueError, KeyError):
                continue
            by_fw[row["label"]].append(row)
    for rows in by_fw.values():
        rows.sort(key=lambda r: r["n_nodes"])
    return by_fw


def plot_runtime_scaling(by_fw, outdir):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    for label, rows in sorted(by_fw.items()):
        xs = [r["n_nodes"] for r in rows]
        ys = [r["time_s"] for r in rows]
        ax.plot(xs, ys, marker="o", markersize=3, label=label)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("Number of nodes")
    ax.set_ylabel("Wall-clock time (s)")
    ax.set_title("Runtime scaling")
    ax.legend()
    return _save(fig, outdir, "runtime_scaling.pdf")


def plot_memory_scaling(by_fw, outdir):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    plotted = False
    for label, rows in sorted(by_fw.items()):
        pts = [(r["n_nodes"], r["peak_mem_mb"]) for r in rows if r["peak_mem_mb"]]
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts],
                    marker="s", markersize=3, label=label)
            plotted = True
    ax.set_xscale("log")
    ax.set_xlabel("Number of nodes")
    ax.set_ylabel("Peak resident memory (MB)")
    ax.set_title("Memory scaling")
    if plotted:
        ax.legend()
    return _save(fig, outdir, "memory_scaling.pdf")


def plot_speedup(by_fw, outdir, baseline="Core (Python)"):
    if baseline not in by_fw:
        return None
    base = {r["n_nodes"]: r["time_s"] for r in by_fw[baseline]}
    fig, ax = plt.subplots(figsize=FIGSIZE)
    for label, rows in sorted(by_fw.items()):
        xs, ys = [], []
        for r in rows:
            if r["n_nodes"] in base and r["time_s"] > 0:
                xs.append(r["n_nodes"])
                ys.append(base[r["n_nodes"]] / r["time_s"])
        if xs:
            ax.plot(xs, ys, marker="^", markersize=3, label=label)
    ax.axhline(1.0, color="gray", linestyle="--", linewidth=0.8)
    ax.set_xscale("log")
    ax.set_xlabel("Number of nodes")
    ax.set_ylabel(f"Speedup vs {baseline}")
    ax.set_title("Relative speedup")
    ax.legend()
    return _save(fig, outdir, "speedup.pdf")


def plot_accuracy(by_fw, outdir):
    labels, diffs = [], []
    for label, rows in sorted(by_fw.items()):
        worst = max((r["max_diff_vs_core"] for r in rows), default=0.0)
        labels.append(label)
        diffs.append(max(worst, 1e-16))   # floor for log scale
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar(range(len(labels)), diffs, color="#4c72b0")
    ax.set_yscale("log")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("max |Δ| vs reference")
    ax.set_title("Numerical agreement (lower = better)")
    return _save(fig, outdir, "accuracy.pdf")


def plot_convergence(input_path, iterations, outdir):
    result = pagerank(read_edges(input_path), damping=config.DAMPING,
                      epsilon=1e-9, max_iter=iterations)
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.semilogy(range(1, len(result.deltas) + 1), result.deltas, marker="o", markersize=3)
    ax.set_xlabel("Iteration")
    ax.set_ylabel("L1 delta")
    ax.set_title(f"Convergence ({result.iterations} iters, N={result.num_nodes})")
    return _save(fig, outdir, "convergence.pdf")


def plot_top_ranks(input_path, outdir, n=10):
    result = pagerank(read_edges(input_path), damping=config.DAMPING,
                      epsilon=1e-9, max_iter=200)
    top = top_n(result.ranks, min(n, result.num_nodes))
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar([k for k, _ in top], [v for _, v in top], color="#2ca02c")
    ax.set_xlabel("Node")
    ax.set_ylabel("PageRank")
    ax.set_title(f"Top {len(top)} nodes")
    return _save(fig, outdir, "top_ranks.pdf")


def _save(fig, outdir, name):
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, name)
    fig.savefig(path, dpi=200)
    plt.close(fig)
    return path


def main():
    parser = argparse.ArgumentParser(description="Generate comparative-study figures.")
    parser.add_argument("--csv", default=config.RESULTS_CSV, help="Benchmark results CSV")
    parser.add_argument("--input", default="data/graph.txt", help="Graph for convergence")
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--outdir", default=config.FIGURES_DIR)
    args = parser.parse_args()

    by_fw = _load_csv(args.csv)
    produced = []
    if by_fw:
        produced += [
            plot_runtime_scaling(by_fw, args.outdir),
            plot_memory_scaling(by_fw, args.outdir),
            plot_speedup(by_fw, args.outdir),
            plot_accuracy(by_fw, args.outdir),
        ]
    else:
        print(f"  (no OK rows in {args.csv}; run tools/benchmark.py first — "
              "skipping scaling/memory/speedup/accuracy figures)")

    produced += [
        plot_convergence(args.input, args.iterations, args.outdir),
        plot_top_ranks(args.input, args.outdir),
    ]

    print(f"Figures written to {args.outdir}/:")
    for path in produced:
        if path:
            print(f"  {os.path.basename(path)}")


if __name__ == "__main__":
    main()
