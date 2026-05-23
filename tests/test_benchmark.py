"""Tests for the comparative benchmark harness (tools/benchmark.py).

These exercise only the dependency-free ``core`` framework so the suite stays
green on a plain Python install and in CI. The focus is the ``--repeat`` feature:
multiple timed runs per (framework, size) aggregated into mean + sample stdev.
"""

import csv
import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_benchmark(args, output_csv):
    """Invoke benchmark.py exactly as a user would; return parsed CSV rows."""
    proc = subprocess.run(
        [sys.executable, "tools/benchmark.py", "--output", str(output_csv), *args],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    assert proc.returncode == 0, (proc.stderr or proc.stdout)[-2000:]
    with open(output_csv, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_repeat_records_mean_and_variance(tmp_path):
    """--repeat N runs the spec N times and records n_runs + the *_std columns."""
    n = 80  # tiny scale-free graph: cheap, deterministic
    graph = os.path.join(PROJECT_ROOT, "data", f"graph_{n}.txt")
    created = not os.path.exists(graph)
    csv_path = tmp_path / "repeat.csv"
    try:
        rows = _run_benchmark(
            ["--generate", "--sizes", str(n), "--frameworks", "core",
             "--repeat", "3", "--iterations", "8"],
            csv_path,
        )
    finally:
        if created and os.path.exists(graph):
            os.remove(graph)

    assert rows, "benchmark produced no rows"
    core = next(r for r in rows if r["framework"] == "core")
    assert core["status"] == "ok"
    assert int(core["n_runs"]) == 3
    # New columns must be present and numeric.
    assert "time_s_std" in core and "peak_mem_mb_std" in core
    float(core["time_s"])
    assert float(core["time_s_std"]) >= 0.0


def test_default_repeat_is_backward_compatible(tmp_path):
    """Without --repeat the harness behaves as a single run (n_runs == 1, std 0)."""
    n = 80
    graph = os.path.join(PROJECT_ROOT, "data", f"graph_{n}.txt")
    created = not os.path.exists(graph)
    csv_path = tmp_path / "single.csv"
    try:
        rows = _run_benchmark(
            ["--generate", "--sizes", str(n), "--frameworks", "core",
             "--iterations", "8"],
            csv_path,
        )
    finally:
        if created and os.path.exists(graph):
            os.remove(graph)

    core = next(r for r in rows if r["framework"] == "core")
    assert core["status"] == "ok"
    assert int(core["n_runs"]) == 1
    assert float(core["time_s_std"]) == 0.0
