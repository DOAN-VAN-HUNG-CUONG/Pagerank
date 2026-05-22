"""
Reference PageRank engine — pure Python, no third-party dependencies.

The engine implements the standard PageRank recurrence (Brin & Page, 1998) with
correct dangling-node handling, and two practical generalisations:

    * weighted PageRank        — edges carry a positive weight; a node's rank is
                                 distributed proportionally to outgoing weights.
    * personalized PageRank    — the teleportation (random-jump) distribution is
                                 an arbitrary probability vector ``p`` instead of
                                 the uniform ``1/N``.

General recurrence (handles all three cases):

    PR(u) = (1 - d) * p(u)
          + d * [  sum_{v -> u}  w(v, u) / W(v) * PR(v)              (link mass)
                 + p(u) * sum_{v in dangling}  PR(v)  ]              (dangling mass)

where
    d          : damping factor (default 0.85)
    p(u)       : teleport probability of node u (uniform 1/N unless personalized)
    W(v)       : total out-weight of v (out-degree in the unweighted case)
    dangling   : nodes with no out-links

This is exactly the formulation used by ``networkx.pagerank`` with the default
``dangling = personalization``, which makes the two directly comparable and lets
the test-suite use NetworkX as an independent ground truth.

Convergence criterion (L1):  sum_u |PR_new(u) - PR_old(u)| < epsilon
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

# An edge is either (src, dst) or (src, dst, weight). Nodes are arbitrary hashables
# but are normalised to ``str`` to match the tab-separated on-disk format.
Edge = Union[Tuple[str, str], Tuple[str, str, float]]

DEFAULT_DAMPING = 0.85
DEFAULT_EPSILON = 1e-3
DEFAULT_MAX_ITER = 100


@dataclass
class PageRankResult:
    """Outcome of a PageRank run.

    Attributes:
        ranks:      node -> PageRank score (scores sum to ~1.0).
        num_nodes:  total number of nodes N.
        iterations: number of iterations actually executed.
        converged:  True if the L1 delta dropped below ``epsilon``.
        deltas:     L1 delta after each iteration (convergence history, useful
                    for plotting).
        damping:    damping factor used.
    """

    ranks: Dict[str, float]
    num_nodes: int
    iterations: int
    converged: bool
    deltas: List[float] = field(default_factory=list)
    damping: float = DEFAULT_DAMPING

    def top(self, n: int = 5) -> List[Tuple[str, float]]:
        """Return the ``n`` highest-ranked nodes as (node, rank) pairs."""
        return top_n(self.ranks, n)

    def total(self) -> float:
        """Sum of all PageRank scores (should be ~1.0 for a valid run)."""
        return sum(self.ranks.values())


# ---------------------------------------------------------------------------
# Graph parsing / construction
# ---------------------------------------------------------------------------
def read_edges(path: str, weighted: bool = False) -> List[Edge]:
    """Read a tab-separated edge list from disk.

    Format per line:
        unweighted:  "src\\tdst"
        weighted:    "src\\tdst\\tweight"

    Blank lines and lines starting with '#' are ignored. Malformed lines are
    skipped silently to mirror the behaviour of the distributed jobs.
    """
    edges: List[Edge] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if weighted:
                if len(parts) < 3:
                    continue
                edges.append((parts[0].strip(), parts[1].strip(), float(parts[2])))
            else:
                if len(parts) < 2:
                    continue
                edges.append((parts[0].strip(), parts[1].strip()))
    return edges


def build_adjacency(
    edges: Iterable[Edge],
    weighted: bool = False,
) -> Tuple[Dict[str, List[Tuple[str, float]]], Dict[str, float]]:
    """Build a weighted adjacency list and per-node total out-weight.

    Returns:
        adjacency: src -> list of (dst, weight) pairs.
        out_weight: src -> total outgoing weight (sum of weights / out-degree).

    Every node that appears as a source *or* a destination is guaranteed to be a
    key in ``adjacency`` (dangling nodes map to an empty list), so the node set
    is complete.
    """
    adjacency: Dict[str, List[Tuple[str, float]]] = {}
    out_weight: Dict[str, float] = {}

    for edge in edges:
        if weighted:
            src, dst, w = edge[0], edge[1], float(edge[2])
        else:
            src, dst, w = edge[0], edge[1], 1.0
        adjacency.setdefault(src, []).append((dst, w))
        adjacency.setdefault(dst, [])  # ensure dst exists (dangling-safe)
        out_weight[src] = out_weight.get(src, 0.0) + w

    # Dangling nodes have zero out-weight.
    for node in adjacency:
        out_weight.setdefault(node, 0.0)

    return adjacency, out_weight


# ---------------------------------------------------------------------------
# Core algorithm
# ---------------------------------------------------------------------------
def pagerank(
    edges: Iterable[Edge],
    damping: float = DEFAULT_DAMPING,
    epsilon: float = DEFAULT_EPSILON,
    max_iter: int = DEFAULT_MAX_ITER,
    weighted: bool = False,
    personalization: Optional[Mapping[str, float]] = None,
    nodes: Optional[Sequence[str]] = None,
) -> PageRankResult:
    """Compute PageRank over a directed graph given as an edge list.

    Args:
        edges: iterable of (src, dst) or (src, dst, weight) tuples.
        damping: damping factor d in [0, 1).
        epsilon: L1 convergence threshold.
        max_iter: maximum number of iterations.
        weighted: if True, the third tuple element is used as the edge weight.
        personalization: optional node -> non-negative score teleport vector. It
            is normalised internally. Missing nodes default to 0. If omitted, a
            uniform 1/N teleport distribution is used (standard PageRank).
        nodes: optional explicit node universe. Useful to inject isolated nodes
            that never appear in any edge.

    Returns:
        PageRankResult with the final scores and the convergence history.

    Raises:
        ValueError: if the graph is empty, damping is out of range, or the
            personalization vector sums to zero over the node set.
    """
    if not (0.0 <= damping < 1.0):
        raise ValueError(f"damping must be in [0, 1), got {damping}")

    adjacency, out_weight = build_adjacency(edges, weighted=weighted)

    if nodes is not None:
        for node in nodes:
            adjacency.setdefault(node, [])
            out_weight.setdefault(node, 0.0)

    node_list = list(adjacency.keys())
    N = len(node_list)
    if N == 0:
        raise ValueError("graph contains no nodes")

    # --- teleport / personalization vector p(u), normalised to sum 1 ---
    if personalization is None:
        teleport = {u: 1.0 / N for u in node_list}
    else:
        total = float(sum(max(0.0, personalization.get(u, 0.0)) for u in node_list))
        if total <= 0.0:
            raise ValueError("personalization vector sums to zero over the node set")
        teleport = {u: max(0.0, personalization.get(u, 0.0)) / total for u in node_list}

    dangling_nodes = [u for u in node_list if out_weight[u] == 0.0]

    # --- initial ranks: uniform 1/N ---
    ranks = {u: 1.0 / N for u in node_list}

    deltas: List[float] = []
    converged = False

    for _ in range(1, max_iter + 1):
        # Mass that dangling nodes redistribute via the teleport vector.
        dangling_mass = sum(ranks[u] for u in dangling_nodes)

        # Base: teleportation + damped redistribution of dangling mass.
        new_ranks = {
            u: (1.0 - damping) * teleport[u] + damping * dangling_mass * teleport[u]
            for u in node_list
        }

        # Link mass: each node pushes rank to its out-neighbours.
        for u in node_list:
            w_total = out_weight[u]
            if w_total <= 0.0:
                continue
            ru = ranks[u]
            for dst, w in adjacency[u]:
                new_ranks[dst] += damping * ru * (w / w_total)

        delta = sum(abs(new_ranks[u] - ranks[u]) for u in node_list)
        deltas.append(delta)
        ranks = new_ranks

        if delta < epsilon:
            converged = True
            break

    return PageRankResult(
        ranks=ranks,
        num_nodes=N,
        iterations=len(deltas),  # one delta recorded per executed iteration
        converged=converged,
        deltas=deltas,
        damping=damping,
    )


def top_n(ranks: Mapping[str, float], n: int = 5) -> List[Tuple[str, float]]:
    """Return the ``n`` highest-ranked (node, rank) pairs.

    Ties are broken by node id to make the output deterministic.
    """
    return sorted(ranks.items(), key=lambda kv: (-kv[1], kv[0]))[:n]
