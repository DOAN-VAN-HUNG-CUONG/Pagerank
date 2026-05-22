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
import os
import random
import sys

import networkx as nx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: E402  (tools/config.py)


def generate(nodes: int, m: int, seed: int, weighted: bool, remove_dangling: bool = True):
    """Build a scale-free directed graph and return its edge list.

    Args:
        nodes: number of nodes.
        m: edges to attach from each new node (Barabasi-Albert parameter).
        seed: RNG seed for reproducibility.
        weighted: if True, attach a random integer weight in [1, 10] per edge.
        remove_dangling: if True (default), give every node at least one out-link
            by adding a back-edge from each would-be dangling node to a random
            other node. This removes the dangling-mass confound so that the
            single-pass (Java/Pig/Streaming) and the mass-redistributing
            (core/mrjob/PySpark) families are numerically comparable on the same
            graph. Set False to study dangling behaviour explicitly.

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

    if remove_dangling:
        sources = {e[0] for e in edges}
        for i in range(nodes):
            node = str(i)
            if node not in sources:
                target = node
                while target == node:
                    target = str(rng.randrange(nodes))
                edges.append((node, target, rng.randint(1, 10)) if weighted
                             else (node, target))
    return edges


def _write_edges(edges, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for edge in edges:
            fh.write("\t".join(str(x) for x in edge) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic graph(s) (edge list).")
    parser.add_argument("--nodes", type=int, default=1000, help="Number of nodes")
    parser.add_argument("--m", type=int, default=config.BA_M,
                        help=f"Barabasi-Albert attachment parameter (default: {config.BA_M})")
    parser.add_argument("--seed", type=int, default=config.SEED, help="RNG seed")
    parser.add_argument("--weighted", action="store_true",
                        help="Emit a third weight column")
    parser.add_argument("--output", default="data/graph_large.txt", help="Output path")
    parser.add_argument("--sizes", type=int, nargs="*", metavar="N",
                        help="Batch mode: generate one graph per size into "
                             "data/graph_<N>.txt (overrides --nodes/--output). "
                             "Defaults to config.GRAPH_SIZES when given with no value.")
    args = parser.parse_args()

    # Batch mode for the scaling study.
    if args.sizes is not None:
        sizes = args.sizes if args.sizes else config.GRAPH_SIZES
        for n in sizes:
            edges = generate(n, args.m, args.seed, args.weighted)
            path = config.graph_path_for(n)
            _write_edges(edges, path)
            print(f"Wrote {len(edges)} edges over {n} nodes to {path}")
        return

    # Single-graph mode.
    edges = generate(args.nodes, args.m, args.seed, args.weighted)
    _write_edges(edges, args.output)
    print(f"Wrote {len(edges)} edges over {args.nodes} nodes to {args.output}"
          + (" (weighted)" if args.weighted else ""))


if __name__ == "__main__":
    main()
