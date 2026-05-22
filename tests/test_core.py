"""Unit tests for the reference engine (python/core).

NetworkX (with SciPy) is used as an independent ground truth. Tests that need it
are skipped automatically when it is not installed.
"""

import pytest
from core import build_adjacency, pagerank, read_edges, top_n

nx = pytest.importorskip("networkx")


def _nx_available_with_scipy() -> bool:
    try:
        import scipy  # noqa: F401
        return True
    except ImportError:
        return False


requires_scipy = pytest.mark.skipif(
    not _nx_available_with_scipy(), reason="NetworkX PageRank backend needs SciPy"
)


# ---------------------------------------------------------------------------
# Agreement with NetworkX
# ---------------------------------------------------------------------------
@requires_scipy
def test_standard_matches_networkx(graph_path):
    edges = read_edges(graph_path)
    result = pagerank(edges, damping=0.85, epsilon=1e-12, max_iter=1000)

    g = nx.DiGraph()
    g.add_edges_from(edges)
    reference = nx.pagerank(g, alpha=0.85, tol=1e-12, max_iter=1000)

    diff = max(abs(result.ranks[n] - reference[n]) for n in reference)
    assert diff < 1e-9


@requires_scipy
def test_personalized_matches_networkx(graph_path):
    edges = read_edges(graph_path)
    personalization = {"1": 1.0}
    result = pagerank(edges, damping=0.85, epsilon=1e-12, max_iter=1000,
                      personalization=personalization)

    g = nx.DiGraph()
    g.add_edges_from(edges)
    reference = nx.pagerank(g, alpha=0.85, personalization={"1": 1.0},
                            tol=1e-12, max_iter=1000)

    diff = max(abs(result.ranks[n] - reference[n]) for n in reference)
    assert diff < 1e-9


@requires_scipy
def test_weighted_matches_networkx(graph_path):
    base = read_edges(graph_path)
    weighted = [(s, d, (i % 5) + 1) for i, (s, d) in enumerate(base)]
    result = pagerank(weighted, damping=0.85, epsilon=1e-12, max_iter=1000, weighted=True)

    g = nx.DiGraph()
    for s, d, w in weighted:
        g.add_edge(s, d, weight=w)
    reference = nx.pagerank(g, alpha=0.85, weight="weight", tol=1e-12, max_iter=1000)

    diff = max(abs(result.ranks[n] - reference[n]) for n in reference)
    assert diff < 1e-9


@requires_scipy
def test_dangling_matches_networkx(dangling_graph_path):
    """The engine must redistribute dangling mass exactly like NetworkX."""
    edges = read_edges(dangling_graph_path)
    result = pagerank(edges, damping=0.85, epsilon=1e-12, max_iter=1000)

    g = nx.DiGraph()
    g.add_edges_from(edges)
    reference = nx.pagerank(g, alpha=0.85, tol=1e-12, max_iter=1000)

    diff = max(abs(result.ranks[n] - reference[n]) for n in reference)
    assert diff < 1e-9


# ---------------------------------------------------------------------------
# Algorithmic invariants (no external dependency)
# ---------------------------------------------------------------------------
def test_ranks_sum_to_one(graph_path):
    result = pagerank(read_edges(graph_path), epsilon=1e-12, max_iter=1000)
    assert abs(result.total() - 1.0) < 1e-9


def test_ranks_sum_to_one_with_dangling(dangling_graph_path):
    result = pagerank(read_edges(dangling_graph_path), epsilon=1e-12, max_iter=1000)
    assert abs(result.total() - 1.0) < 1e-9


def test_all_nodes_present_including_dangling(dangling_graph_path):
    edges = read_edges(dangling_graph_path)
    adjacency, _ = build_adjacency(edges)
    result = pagerank(edges, epsilon=1e-12, max_iter=1000)
    assert set(result.ranks) == set(adjacency)
    # Dangling nodes (4 and 6) must still receive a positive rank.
    assert result.ranks["4"] > 0
    assert result.ranks["6"] > 0


def test_convergence_history_recorded_and_decreasing(graph_path):
    result = pagerank(read_edges(graph_path), epsilon=1e-3, max_iter=100)
    assert result.converged
    assert len(result.deltas) == result.iterations
    # The L1 delta should be (weakly) decreasing for this well-behaved graph.
    assert all(b <= a + 1e-12 for a, b in zip(result.deltas, result.deltas[1:]))


def test_higher_damping_changes_ranking(graph_path):
    edges = read_edges(graph_path)
    low = pagerank(edges, damping=0.50, epsilon=1e-12, max_iter=1000)
    high = pagerank(edges, damping=0.95, epsilon=1e-12, max_iter=1000)
    # Different damping must yield different scores (sanity check).
    assert max(abs(low.ranks[n] - high.ranks[n]) for n in low.ranks) > 1e-3


def test_top_n_is_deterministic_and_sorted(graph_path):
    result = pagerank(read_edges(graph_path), epsilon=1e-12, max_iter=1000)
    top = top_n(result.ranks, 3)
    assert len(top) == 3
    assert top == sorted(top, key=lambda kv: (-kv[1], kv[0]))


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------
def test_invalid_damping_raises():
    with pytest.raises(ValueError):
        pagerank([("1", "2")], damping=1.5)


def test_empty_graph_raises():
    with pytest.raises(ValueError):
        pagerank([])


def test_zero_personalization_raises(graph_path):
    edges = read_edges(graph_path)
    with pytest.raises(ValueError):
        pagerank(edges, personalization={"does-not-exist": 1.0})
