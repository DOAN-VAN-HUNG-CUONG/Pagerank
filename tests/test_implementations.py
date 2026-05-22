"""Cross-implementation consistency tests.

Each distributed implementation is compared against the verified reference
engine (python/core). Implementations whose runtime is not installed
(mrjob / pyspark) are skipped automatically, so the suite is green on a plain
Python install and exercises everything available on a full big-data box.

Comparison strategy:
    * mrjob (both engines) early-stop at EPSILON=0.001, exactly like the engine,
      so they are compared to ``core`` with the same epsilon.
    * Hadoop Streaming runs a fixed number of iterations with no early stop, so
      it is compared to a fully-converged ``core`` run.
All equality tests use ``data/graph.txt`` (no dangling nodes), where every
implementation — including the single-pass JVM/streaming family — is exact.
"""

import os
import shutil
import subprocess
import sys

import pytest
from conftest import load_ranks, max_abs_diff
from core import pagerank, read_edges

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# mrjob early-stop threshold, kept in sync with python/mrjob/pagerank_mrjob.py.
MRJOB_EPSILON = 0.001


def _converged(graph_path, epsilon=1e-10, max_iter=300):
    return pagerank(read_edges(graph_path), damping=0.85, epsilon=epsilon, max_iter=max_iter)


def _run_mrjob_cli(graph_path, out, engine):
    """Invoke the mrjob script exactly as a user would (real entry point)."""
    script = os.path.join(PROJECT_ROOT, "python", "mrjob", "pagerank_mrjob.py")
    subprocess.run(
        [sys.executable, script, graph_path, "--iterations", "100",
         "--engine", engine, "--output", str(out)],
        check=True, cwd=PROJECT_ROOT, capture_output=True,
    )


# ---------------------------------------------------------------------------
# mrjob
# ---------------------------------------------------------------------------
@pytest.mark.mrjob
def test_mrjob_core_engine_matches_reference(graph_path, tmp_path):
    pytest.importorskip("mrjob")
    out = tmp_path / "core.txt"
    _run_mrjob_cli(graph_path, out, engine="core")
    ranks = load_ranks(out)
    ref = pagerank(read_edges(graph_path), damping=0.85, epsilon=MRJOB_EPSILON, max_iter=100)
    # Output files store 8-decimal ranks, so rounding caps agreement at ~5e-9.
    assert max_abs_diff(ranks, ref.ranks) < 1e-7


@pytest.mark.mrjob
def test_mrjob_mapreduce_engine_matches_reference(graph_path, tmp_path):
    pytest.importorskip("mrjob")
    out = tmp_path / "mr.txt"
    _run_mrjob_cli(graph_path, out, engine="mrjob")
    ranks = load_ranks(out)
    ref = pagerank(read_edges(graph_path), damping=0.85, epsilon=MRJOB_EPSILON, max_iter=100)
    assert max_abs_diff(ranks, ref.ranks) < 1e-6


# ---------------------------------------------------------------------------
# Hadoop Streaming (local simulation through the shell pipeline)
# ---------------------------------------------------------------------------
def test_streaming_local_simulation_matches_reference(graph_path):
    iterations = 60
    script = os.path.join(PROJECT_ROOT, "scripts", "run_streaming.sh")
    subprocess.run(
        ["bash", script, str(iterations), "--local", graph_path],
        check=True, cwd=PROJECT_ROOT, capture_output=True,
    )
    out = os.path.join(PROJECT_ROOT, "output", "pagerank_streaming.txt")
    ranks = load_ranks(out)
    ref = _converged(graph_path)
    assert max_abs_diff(ranks, ref.ranks) < 1e-4


# ---------------------------------------------------------------------------
# PySpark (RDD + DataFrame)
# ---------------------------------------------------------------------------
def _read_spark_text_dir(directory: str) -> dict:
    """Read 'node\\trank' part files emitted by saveAsTextFile."""
    ranks = {}
    for name in os.listdir(directory):
        if name.startswith("_") or name.endswith(".crc"):
            continue
        with open(os.path.join(directory, name), encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                node, rank = line.split("\t")
                ranks[node] = float(rank)
    return ranks


@pytest.mark.spark
def test_pyspark_matches_reference(graph_path, tmp_path):
    pytest.importorskip("pyspark")
    spark_submit = shutil.which("spark-submit")
    if spark_submit is None:
        pytest.skip("spark-submit not available on PATH")

    out_prefix = tmp_path / "spark"
    script = os.path.join(PROJECT_ROOT, "python", "pyspark", "pagerank_spark.py")
    proc = subprocess.run(
        [spark_submit, "--master", "local[*]", script,
         "--input", graph_path, "--mode", "both",
         "--iterations", "30", "--output", str(out_prefix)],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        # Surface the real Spark error instead of a bare non-zero exit code.
        # Spark logs are verbose, so extract the lines that actually describe the
        # failure and also keep a tail as a fallback.
        combined = (proc.stdout or "") + "\n----- STDERR -----\n" + (proc.stderr or "")
        keywords = ("Error", "Exception", "Traceback", "Caused by",
                    "AnalysisException", "py4j", "ERROR", "raise ")
        hits = [ln for ln in combined.splitlines() if any(k in ln for k in keywords)]
        excerpt = "\n".join(hits[-50:]) if hits else "(no error-like lines found)"
        pytest.fail(
            "spark-submit failed (exit %d).\n=== error lines ===\n%s\n\n=== tail ===\n%s"
            % (proc.returncode, excerpt, combined[-2500:])
        )

    # Both PySpark paths early-stop at the implementation's EPSILON=0.001, exactly
    # like the reference engine, so compare against core at the SAME epsilon (not a
    # fully-converged run, which would differ by ~epsilon). The delta sequence
    # straddles 0.001 with a clear margin (iter 12 ~0.00158, iter 13 ~0.00093), so
    # every implementation stops at the same iteration with matching ranks.
    ref = pagerank(read_edges(graph_path), damping=0.85, epsilon=0.001, max_iter=100)
    rdd_ranks = _read_spark_text_dir(str(out_prefix) + "/result_rdd")
    assert max_abs_diff(rdd_ranks, ref.ranks) < 1e-6

    df_dir = str(out_prefix) + "/result_df"
    df_ranks = _read_spark_text_dir(df_dir)  # DF csv with sep='\t' parses the same
    assert max_abs_diff(df_ranks, ref.ranks) < 1e-6


# ---------------------------------------------------------------------------
# Scalability sanity check on a larger synthetic scale-free graph
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_core_matches_networkx_on_scale_free_graph():
    nx = pytest.importorskip("networkx")
    pytest.importorskip("scipy")

    g = nx.barabasi_albert_graph(400, 3, seed=11)
    edges = []
    for u, v in g.edges():
        edges.append((str(min(u, v)), str(max(u, v))))

    result = pagerank(edges, damping=0.85, epsilon=1e-12, max_iter=2000)

    dg = nx.DiGraph()
    dg.add_edges_from(edges)
    reference = nx.pagerank(dg, alpha=0.85, tol=1e-12, max_iter=2000)

    assert max(abs(result.ranks[n] - reference[n]) for n in reference) < 1e-9
