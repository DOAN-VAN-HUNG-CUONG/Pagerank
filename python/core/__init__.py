"""
Reference PageRank engine (pure Python, dependency-free).

This package provides the canonical, well-tested implementation that every
distributed implementation (mrjob, PySpark, Hadoop Streaming, Java, Pig) is
validated against. Keeping a single source of truth guarantees that the five
big-data variants stay numerically consistent.
"""

from .pagerank_core import (
    PageRankResult,
    build_adjacency,
    pagerank,
    read_edges,
    top_n,
)

__all__ = [
    "PageRankResult",
    "pagerank",
    "read_edges",
    "build_adjacency",
    "top_n",
]

__version__ = "2.0.0"
