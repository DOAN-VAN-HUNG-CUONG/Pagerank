"""
Central configuration for the PageRank comparative benchmark.

Edit ``GRAPH_SIZES`` and ``FRAMEWORKS`` here; ``benchmark.py`` and ``visualize.py``
read from this module so the whole study is driven from one place.
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "docs", "figures")

# Primary machine-readable benchmark output (one row per framework x graph size).
RESULTS_CSV = os.path.join(OUTPUT_DIR, "benchmark_results.csv")

# Algorithm parameters (kept in sync with the implementations).
DAMPING = 0.85
EPSILON = 0.001
DEFAULT_ITERATIONS = 50

# Node counts for the scaling study. Small sizes are cheap and run anywhere;
# larger sizes (1e5+) are meant for the cluster runs that produce the paper's
# figures. Trim or extend to match your hardware.
GRAPH_SIZES = [1000, 5000, 10000, 50000, 100000]

# Barabasi-Albert generator parameters (see tools/generate_graph.py).
BA_M = 3
SEED = 42

# Framework registry. ``requires`` names an executable or importable module that
# must be present; benchmark.py auto-skips a framework whose requirement is
# missing (so the same command works on a laptop and on a full big-data box).
FRAMEWORKS = {
    "core":            {"label": "Core (Python)",     "requires": None,          "kind": "python"},
    "mrjob_core":      {"label": "mrjob (core)",      "requires": "mrjob",       "kind": "module"},
    "mrjob_mapreduce": {"label": "mrjob (MapReduce)", "requires": "mrjob",       "kind": "module"},
    "streaming":       {"label": "Hadoop Streaming",  "requires": "bash",        "kind": "exe"},
    "pyspark_rdd":     {"label": "PySpark RDD",       "requires": "spark-submit","kind": "exe"},
    "pyspark_df":      {"label": "PySpark DataFrame", "requires": "spark-submit","kind": "exe"},
}

# Default subset to run when ``--frameworks`` is not given (everything available).
DEFAULT_FRAMEWORKS = list(FRAMEWORKS.keys())


def graph_path_for(n_nodes: int) -> str:
    """Canonical path of the generated graph with ``n_nodes`` nodes."""
    return os.path.join(DATA_DIR, f"graph_{n_nodes}.txt")
