"""Shared pytest fixtures and path setup for the PageRank test-suite."""

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Make the dependency-free reference engine importable as ``core``.
sys.path.insert(0, os.path.join(PROJECT_ROOT, "python"))


@pytest.fixture(scope="session")
def project_root() -> str:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def graph_path() -> str:
    """Edge list with NO dangling nodes (every node has an out-link)."""
    return os.path.join(PROJECT_ROOT, "data", "graph.txt")


@pytest.fixture(scope="session")
def dangling_graph_path() -> str:
    """Edge list that contains dangling nodes (nodes with no out-link)."""
    return os.path.join(PROJECT_ROOT, "data", "graph_dangling.txt")


def load_ranks(path: str):
    """Load a 'node\\tpagerank' result file into a dict (skips the header)."""
    ranks = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("node"):
                continue
            node, rank = line.split("\t")
            ranks[node] = float(rank)
    return ranks


def max_abs_diff(a: dict, b: dict) -> float:
    """Maximum absolute per-node difference between two rank dicts."""
    assert set(a) == set(b), "node sets differ"
    return max(abs(a[n] - b[n]) for n in a)
