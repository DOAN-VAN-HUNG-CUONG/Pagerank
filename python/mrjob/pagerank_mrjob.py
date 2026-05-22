"""
PageRank with mrjob.

Two execution paths share this file:

  1. ``--engine core`` (default): runs the verified pure-Python reference engine
     (python/core). This is exact (handles dangling mass) and is what the
     ``--engine core`` CLI and the test-suite use. Output is byte-identical to
     previous releases of this script.

  2. ``--engine mrjob``: drives the ``PageRankIteration`` MapReduce job once per
     iteration through an mrjob runner (``inline`` / ``local`` / ``hadoop``).
     This path demonstrates the genuine map/reduce decomposition and can run on
     a real Hadoop cluster with ``--runner hadoop``.

Usage:
  # Reference engine (recommended, exact):
  python3 pagerank_mrjob.py data/graph.txt --iterations 20

  # MapReduce decomposition, local simulation:
  python3 pagerank_mrjob.py data/graph.txt --iterations 20 --engine mrjob

  # On a Hadoop cluster:
  python3 pagerank_mrjob.py hdfs:///graph.txt --engine mrjob --runner hadoop

Algorithm:
  PR(u) = (1 - d)/N + d * sum(PR(v) / out_degree(v))   for all v -> u,  d = 0.85

NOTE on dangling nodes:
  The ``PageRankIteration`` MapReduce job uses the standard single-pass formula,
  which (like the Java, Pig and Hadoop-Streaming jobs) drops the mass of dangling
  nodes. It is therefore exact only for graphs in which every node has at least
  one out-link. The ``core`` engine redistributes dangling mass and is exact for
  every graph. See docs/METHODOLOGY.md.
"""

import argparse
import io
import os
import sys

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

# Make the dependency-free reference engine importable regardless of CWD.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ""))
from core import build_adjacency, pagerank, read_edges, top_n  # noqa: E402

DAMPING = 0.85
EPSILON = 0.001
MAX_ITER = 20


# =============================================================================
# MapReduce building block: one PageRank iteration
# =============================================================================
class PageRankIteration(MRJob):
    """One PageRank iteration as a single MapReduce step.

    Input  (JSON lines):  node \\t [rank, [neighbor, ...]]
    Output (JSON lines):  node \\t [new_rank, [neighbor, ...]]

    Mapper emits two record types:
        (node,     ["graph", neighbors])  -- preserve adjacency for the reducer
        (neighbor, ["rank", share])       -- rank contribution to each out-link
    Reducer applies:  new_rank = (1 - d)/N + d * sum(shares)
    """

    INPUT_PROTOCOL = JSONProtocol
    INTERNAL_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--num-nodes", type=int, default=1,
                              help="Total number of nodes N")
        self.add_passthru_arg("--damping", type=float, default=DAMPING,
                              help="Damping factor d")

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, node, value):
        rank, neighbors = value
        # Preserve graph structure for the reducer.
        yield node, ["graph", neighbors]
        # Distribute rank equally to each out-neighbour.
        if neighbors:
            share = rank / len(neighbors)
            for nb in neighbors:
                yield nb, ["rank", share]
        # Dangling nodes (no out-links): mass is dropped in this single-pass form.

    def reducer(self, node, values):
        neighbors = []
        rank_sum = 0.0
        for vtype, val in values:
            if vtype == "graph":
                neighbors = val
            elif vtype == "rank":
                rank_sum += val
        N = self.options.num_nodes
        d = self.options.damping
        new_rank = (1.0 - d) / N + d * rank_sum
        yield node, [new_rank, neighbors]


# =============================================================================
# Engine 1 — pure-Python reference (exact)
# =============================================================================
def run_pagerank_local(input_file, iterations, output_file):
    """Run PageRank with the verified reference engine and write sorted output."""
    print(f"[PageRank mrjob/core] Reading graph from: {input_file}")
    edges = read_edges(input_file)
    result = pagerank(edges, damping=DAMPING, epsilon=EPSILON, max_iter=iterations)

    print(f"[PageRank mrjob/core] Nodes: {result.num_nodes}, Damping: {DAMPING}, "
          f"Max iter: {iterations}")
    for i, delta in enumerate(result.deltas, start=1):
        print(f"  Iteration {i:2d} | delta = {delta:.6f}")
    if result.converged:
        print(f"  Converged after {result.iterations} iterations.")

    _write_and_report(result.ranks, output_file)
    return result.ranks


# =============================================================================
# Engine 2 — iterative MapReduce via mrjob
# =============================================================================
def run_with_mrjob(input_file, iterations, output_file, runner="inline"):
    """Drive PageRankIteration once per iteration through an mrjob runner.

    State is kept as JSON lines "node\\t[rank, neighbors]" and streamed through
    the job each iteration. Convergence (L1 delta) is checked in the driver.
    """
    print(f"[PageRank mrjob/MR] Reading graph from: {input_file}")
    edges = read_edges(input_file)
    weighted_adj, _ = build_adjacency(edges)
    # The MapReduce job works with plain out-neighbour id lists (unweighted).
    adjacency = {node: [dst for dst, _w in nbrs] for node, nbrs in weighted_adj.items()}
    N = len(adjacency)
    print(f"[PageRank mrjob/MR] Nodes: {N}, Damping: {DAMPING}, runner: {runner}")

    # Initial state lines: node \t [1/N, [neighbors]]  (JSON encoded).
    ranks = {node: 1.0 / N for node in adjacency}
    state_lines = _encode_state(adjacency, ranks)

    for iteration in range(1, iterations + 1):
        job = PageRankIteration(args=[
            "-r", runner, "--num-nodes", str(N), "--damping", str(DAMPING),
        ])
        job.sandbox(stdin=io.BytesIO(_lines_to_bytes(state_lines)))
        new_ranks = {}
        adjacency_out = {}
        with job.make_runner() as job_runner:
            job_runner.run()
            for node, value in job.parse_output(job_runner.cat_output()):
                new_ranks[node] = value[0]
                adjacency_out[node] = value[1]

        delta = sum(abs(new_ranks[n] - ranks[n]) for n in ranks)
        ranks = new_ranks
        adjacency = adjacency_out
        state_lines = _encode_state(adjacency, ranks)
        print(f"  Iteration {iteration:2d} | delta = {delta:.6f}")
        if delta < EPSILON:
            print(f"  Converged after {iteration} iterations.")
            break

    _write_and_report(ranks, output_file)
    return ranks


def _encode_state(adjacency, ranks):
    """Encode {node: [rank, neighbors]} as JSON 'key\\tvalue' lines for mrjob."""
    import json
    return [
        f"{json.dumps(node)}\t{json.dumps([ranks[node], adjacency.get(node, [])])}"
        for node in adjacency
    ]


def _lines_to_bytes(lines):
    return ("\n".join(lines) + "\n").encode("utf-8")


# =============================================================================
# Shared output helper
# =============================================================================
def _write_and_report(ranks, output_file):
    ranked = top_n(ranks, len(ranks))
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as fh:
        fh.write("node\tpagerank\n")
        for node, rank in ranked:
            fh.write(f"{node}\t{rank:.8f}\n")

    print(f"\n[PageRank mrjob] Results written to: {output_file}")
    print("\nTop-5 nodes by PageRank:")
    print(f"{'Node':<10} {'PageRank':<12}")
    print("-" * 22)
    for node, rank in ranked[:5]:
        print(f"{node:<10} {rank:.8f}")


# =============================================================================
# Entry point
# =============================================================================
# When mrjob launches a task (mapper/reducer subprocess), it re-invokes this file
# and expects MRJob.run() to take over. Detect that and dispatch to the job.
def _is_mrjob_task_invocation(argv):
    return any(a.startswith("--step-num") or a in ("--mapper", "--reducer", "--combiner")
               for a in argv)


if __name__ == "__main__":
    if _is_mrjob_task_invocation(sys.argv[1:]):
        PageRankIteration.run()
    else:
        parser = argparse.ArgumentParser(description="PageRank via mrjob")
        parser.add_argument("input", help="Input edge list file (tab-separated)")
        parser.add_argument("--iterations", type=int, default=MAX_ITER,
                            help=f"Max iterations (default: {MAX_ITER})")
        parser.add_argument("--output", default="output/pagerank_mrjob.txt",
                            help="Output file path")
        parser.add_argument("--engine", choices=["core", "mrjob"], default="core",
                            help="core = exact reference engine (default); "
                                 "mrjob = iterative MapReduce job")
        parser.add_argument("--runner", choices=["inline", "local", "hadoop"],
                            default="inline",
                            help="mrjob runner for --engine mrjob")
        args = parser.parse_args()

        if args.engine == "core":
            run_pagerank_local(args.input, args.iterations, args.output)
        else:
            run_with_mrjob(args.input, args.iterations, args.output, runner=args.runner)
