"""
Command-line interface for the reference PageRank engine.

Examples:
    python -m core.cli data/graph.txt --iterations 20
    python -m core.cli data/graph_weighted.txt --weighted --damping 0.9
    python -m core.cli data/graph.txt --personalize 1=1.0 --output output/ppr.txt

The output file format matches the other implementations:
    node<TAB>pagerank          (header)
    <node><TAB><rank:.8f>       (rows, sorted by rank descending)
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, Optional

from .pagerank_core import (
    DEFAULT_DAMPING,
    DEFAULT_EPSILON,
    DEFAULT_MAX_ITER,
    pagerank,
    read_edges,
    top_n,
)


def _parse_personalization(items: Optional[list]) -> Optional[Dict[str, float]]:
    """Parse ``--personalize node=score`` pairs into a dict."""
    if not items:
        return None
    vector: Dict[str, float] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"--personalize expects node=score, got: {item}")
        node, score = item.split("=", 1)
        vector[node.strip()] = float(score)
    return vector


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pagerank",
        description="Reference PageRank engine (standard / weighted / personalized).",
    )
    parser.add_argument("input", help="Input edge list (tab-separated)")
    parser.add_argument(
        "--output",
        default="output/pagerank_core.txt",
        help="Output file path (default: output/pagerank_core.txt)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_MAX_ITER,
        help=f"Maximum iterations (default: {DEFAULT_MAX_ITER})",
    )
    parser.add_argument(
        "--damping",
        type=float,
        default=DEFAULT_DAMPING,
        help=f"Damping factor (default: {DEFAULT_DAMPING})",
    )
    parser.add_argument(
        "--epsilon",
        type=float,
        default=DEFAULT_EPSILON,
        help=f"L1 convergence threshold (default: {DEFAULT_EPSILON})",
    )
    parser.add_argument(
        "--weighted",
        action="store_true",
        help="Treat the 3rd column as an edge weight",
    )
    parser.add_argument(
        "--personalize",
        nargs="+",
        metavar="NODE=SCORE",
        help="Personalized teleport vector, e.g. --personalize 1=1.0 3=0.5",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="How many top nodes to print (default: 5)",
    )
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)

    print(f"[PageRank core] Reading graph: {args.input}")
    edges = read_edges(args.input, weighted=args.weighted)
    if not edges:
        print("[PageRank core] No valid edges found.", file=sys.stderr)
        return 1

    personalization = _parse_personalization(args.personalize)

    result = pagerank(
        edges,
        damping=args.damping,
        epsilon=args.epsilon,
        max_iter=args.iterations,
        weighted=args.weighted,
        personalization=personalization,
    )

    print(
        f"[PageRank core] Nodes: {result.num_nodes} | Damping: {result.damping} | "
        f"Max iter: {args.iterations}"
    )
    for i, delta in enumerate(result.deltas, start=1):
        print(f"  Iteration {i:2d} | delta = {delta:.6f}")
    if result.converged:
        print(f"  Converged after {result.iterations} iterations.")
    else:
        print(f"  Reached max_iter ({args.iterations}) without converging.")

    # Write sorted results.
    import os

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    ranked = top_n(result.ranks, len(result.ranks))
    with open(args.output, "w", encoding="utf-8") as fh:
        fh.write("node\tpagerank\n")
        for node, rank in ranked:
            fh.write(f"{node}\t{rank:.8f}\n")

    print(f"\n[PageRank core] Results written to: {args.output}")
    print(f"Sum of ranks (sanity check): {result.total():.8f}")
    print(f"\n{'Node':<10} {'PageRank':<12}")
    print("-" * 22)
    for node, rank in result.top(args.top):
        print(f"{node:<10} {rank:.8f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
