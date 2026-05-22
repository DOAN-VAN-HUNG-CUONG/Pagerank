"""
Generate a synthetic directed graph as a tab-separated edge list.

Real web graphs are scale-free (their degree distribution follows a power law),
so we use the Barabasi-Albert preferential-attachment model and orient each edge
from the lower-id endpoint to the higher-id endpoint. This produces a directed
acyclic-ish graph with a few high-degree "hub" nodes — a realistic stress test
for the PageRank implementations.

Usage:
    python tools/generate_graph.py --nodes 1000 --output data/graph_large.txt
    python tools/generate_graph.py --nodes 5000 --m 3 --weighted --seed 7
"""

import argparse
import random

import networkx as nx


def generate(nodes: int, m: int, seed: int, weighted: bool):
    """Build a scale-free directed graph and return its edge list.

    Args:
        nodes: number of nodes.
        m: edges to attach from each new node (Barabasi-Albert parameter).
        seed: RNG seed for reproducibility.
        weighted: if True, attach a random integer weight in [1, 10] per edge.

    Returns:
        list of (src, dst) or (src, dst, weight) tuples (ids as strings).
    """
    if nodes <= m:
        raise ValueError(f"--nodes ({nodes}) must be greater than --m ({m})")

    ba = nx.barabasi_albert_graph(nodes, m, seed=seed)
    rng = random.Random(seed)

    edges = []
    for u, v in ba.edges():
        src, dst = (u, v) if u < v else (v, u)  # orient low -> high
        if weighted:
            edges.append((str(src), str(dst), rng.randint(1, 10)))
        else:
            edges.append((str(src), str(dst)))
    return edges


def main():
    parser = argparse.ArgumentParser(description="Generate a synthetic graph (edge list).")
    parser.add_argument("--nodes", type=int, default=1000, help="Number of nodes")
    parser.add_argument("--m", type=int, default=2,
                        help="Barabasi-Albert attachment parameter (default: 2)")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument("--weighted", action="store_true",
                        help="Emit a third weight column")
    parser.add_argument("--output", default="data/graph_large.txt", help="Output path")
    args = parser.parse_args()

    edges = generate(args.nodes, args.m, args.seed, args.weighted)

    with open(args.output, "w", encoding="utf-8") as fh:
        for edge in edges:
            fh.write("\t".join(str(x) for x in edge) + "\n")

    print(f"Wrote {len(edges)} edges over {args.nodes} nodes to {args.output}"
          + (" (weighted)" if args.weighted else ""))


if __name__ == "__main__":
    main()
