"""
Generate figures for the PageRank project from real computed data.

Figures (written to docs/figures/ as PNG):
    1. convergence.png  — L1 delta per iteration (semi-log), from the reference engine.
    2. top_ranks.png    — bar chart of the highest-ranked nodes.
    3. graph.png        — network drawing with node size/colour proportional to
                          PageRank (drawn only for small graphs, N <= 60).
    4. runtime.png      — wall-clock comparison across available implementations,
                          read from a benchmark JSON report when present.

Usage:
    python tools/visualize.py --input data/graph.txt --iterations 50
    python tools/visualize.py --input data/graph.txt --benchmark output/benchmark.json
"""

import argparse
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")  # headless backend — no display required
import matplotlib.pyplot as plt  # noqa: E402

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "python"))

from core import pagerank, read_edges, top_n  # noqa: E402


def plot_convergence(result, outdir):
    fig, ax = plt.subplots(figsize=(7, 4.2))
    iters = range(1, len(result.deltas) + 1)
    ax.semilogy(iters, result.deltas, marker="o", color="#1f77b4")
    ax.set_xlabel("Iteration")
    ax.set_ylabel("L1 delta  (sum |PR_new - PR_old|)")
    ax.set_title(f"PageRank convergence — converged in {result.iterations} iterations")
    ax.grid(True, which="both", linestyle=":", alpha=0.6)
    fig.tight_layout()
    path = os.path.join(outdir, "convergence.png")
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def plot_top_ranks(result, outdir, n=10):
    top = top_n(result.ranks, min(n, len(result.ranks)))
    nodes = [k for k, _ in top]
    scores = [v for _, v in top]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.bar(nodes, scores, color="#2ca02c")
    ax.set_xlabel("Node")
    ax.set_ylabel("PageRank")
    ax.set_title(f"Top {len(top)} nodes by PageRank")
    for x, y in zip(nodes, scores):
        ax.text(x, y, f"{y:.3f}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    path = os.path.join(outdir, "top_ranks.png")
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def plot_graph(edges, result, outdir, max_nodes=60):
    if result.num_nodes > max_nodes:
        print(f"  graph.png skipped (N={result.num_nodes} > {max_nodes})")
        return None
    try:
        import networkx as nx
    except ImportError:
        print("  graph.png skipped (networkx not installed)")
        return None

    g = nx.DiGraph()
    g.add_edges_from([(s, d) for s, d in edges])
    pos = nx.spring_layout(g, seed=42)
    ranks = result.ranks
    sizes = [3000 * ranks[node] for node in g.nodes()]
    colors = [ranks[node] for node in g.nodes()]

    fig, ax = plt.subplots(figsize=(7, 6))
    nodes = nx.draw_networkx_nodes(g, pos, node_size=sizes, node_color=colors,
                                   cmap="viridis", ax=ax)
    nx.draw_networkx_edges(g, pos, alpha=0.35, arrows=True,
                           arrowstyle="-|>", arrowsize=10, ax=ax)
    nx.draw_networkx_labels(g, pos, font_size=9, font_color="white", ax=ax)
    fig.colorbar(nodes, ax=ax, label="PageRank")
    ax.set_title("Graph — node size & colour proportional to PageRank")
    ax.axis("off")
    fig.tight_layout()
    path = os.path.join(outdir, "graph.png")
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def plot_runtime(benchmark_path, outdir):
    if not os.path.isfile(benchmark_path):
        print(f"  runtime.png skipped (no benchmark report at {benchmark_path})")
        return None
    with open(benchmark_path, encoding="utf-8") as fh:
        report = json.load(fh)

    labels = ["core"]
    times = [report["core"]["time_s"]]
    for name, entry in report["implementations"].items():
        if entry.get("available"):
            labels.append(name)
            times.append(entry["time_s"])

    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.barh(labels, times, color="#ff7f0e")
    ax.set_xlabel("Wall-clock time (s)")
    ax.set_title(f"Runtime — {report['input']} "
                 f"(N={report['num_nodes']}, max_iter={report['iterations']})")
    for i, t in enumerate(times):
        ax.text(t, i, f" {t:.3f}s", va="center", fontsize=8)
    fig.tight_layout()
    path = os.path.join(outdir, "runtime.png")
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def main():
    parser = argparse.ArgumentParser(description="Generate PageRank figures.")
    parser.add_argument("--input", default="data/graph.txt", help="Input edge list")
    parser.add_argument("--iterations", type=int, default=50, help="Max iterations")
    parser.add_argument("--epsilon", type=float, default=1e-6, help="Convergence threshold")
    parser.add_argument("--benchmark", default="output/benchmark.json",
                        help="Benchmark JSON report for the runtime chart")
    parser.add_argument("--outdir", default="docs/figures", help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    edges = read_edges(args.input)
    result = pagerank(edges, damping=0.85, epsilon=args.epsilon, max_iter=args.iterations)

    print(f"Generating figures for {args.input} (N={result.num_nodes}) -> {args.outdir}/")
    produced = [
        plot_convergence(result, args.outdir),
        plot_top_ranks(result, args.outdir),
        plot_graph(edges, result, args.outdir),
        plot_runtime(args.benchmark, args.outdir),
    ]
    for path in produced:
        if path:
            print(f"  wrote {path}")


if __name__ == "__main__":
    main()
