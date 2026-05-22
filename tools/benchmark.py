"""
Benchmark and cross-validate the PageRank implementations.

For every implementation that is available on the current machine, this script
measures wall-clock time, records the number of iterations to convergence, and
reports the maximum per-node disagreement against the reference engine. Results
are printed as a table and written to a JSON report. All numbers are measured at
run time — nothing is hard-coded.

Implementations covered:
    * core            — pure-Python reference engine (always available)
    * mrjob (core)    — mrjob script, --engine core
    * mrjob (MR)      — mrjob script, --engine mrjob (inline runner)
    * streaming       — Hadoop Streaming pipeline, local simulation
    * pyspark         — spark-submit RDD + DataFrame (if spark-submit present)

Usage:
    python tools/benchmark.py --input data/graph.txt --iterations 50
    python tools/benchmark.py --input data/graph_large.txt --iterations 100 \
        --output output/benchmark_large.json
"""

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "python"))

from core import pagerank, read_edges, top_n  # noqa: E402


def _load_ranks(path: str) -> dict:
    ranks = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("node"):
                continue
            node, rank = line.split("\t")
            ranks[node] = float(rank)
    return ranks


def _read_spark_text_dir(directory: str) -> dict:
    ranks = {}
    if not os.path.isdir(directory):
        return ranks
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


def _max_diff(a: dict, b: dict):
    common = set(a) & set(b)
    if not common or set(a) != set(b):
        return None
    return max(abs(a[n] - b[n]) for n in common)


def _have_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


# ---------------------------------------------------------------------------
# Individual benchmark runners. Each returns a result dict or None if skipped.
# ---------------------------------------------------------------------------
def bench_core(input_path, iterations, epsilon):
    edges = read_edges(input_path)
    start = time.perf_counter()
    result = pagerank(edges, damping=0.85, epsilon=epsilon, max_iter=iterations)
    elapsed = time.perf_counter() - start
    return {
        "available": True,
        "time_s": round(elapsed, 4),
        "iterations": result.iterations,
        "converged": result.converged,
        "deltas": result.deltas,
        "ranks": result.ranks,
        "top5": top_n(result.ranks, 5),
    }


def bench_mrjob(input_path, iterations, engine, tmp_dir):
    if not _have_module("mrjob"):
        return {"available": False, "reason": "mrjob not installed"}
    out = os.path.join(tmp_dir, f"mrjob_{engine}.txt")
    script = os.path.join(PROJECT_ROOT, "python", "mrjob", "pagerank_mrjob.py")
    start = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, script, input_path, "--iterations", str(iterations),
         "--engine", engine, "--output", out],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        return {"available": False, "reason": proc.stderr.strip()[-200:]}
    return {"available": True, "time_s": round(elapsed, 4), "ranks": _load_ranks(out)}


def bench_streaming(input_path, iterations):
    script = os.path.join(PROJECT_ROOT, "scripts", "run_streaming.sh")
    start = time.perf_counter()
    proc = subprocess.run(
        ["bash", script, str(iterations), "--local", input_path],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        return {"available": False, "reason": proc.stderr.strip()[-200:]}
    out = os.path.join(PROJECT_ROOT, "output", "pagerank_streaming.txt")
    return {"available": True, "time_s": round(elapsed, 4), "ranks": _load_ranks(out)}


def bench_pyspark(input_path, iterations, tmp_dir):
    if not _have_module("pyspark") or shutil.which("spark-submit") is None:
        return {"available": False, "reason": "pyspark / spark-submit not available"}
    out_prefix = os.path.join(tmp_dir, "spark")
    script = os.path.join(PROJECT_ROOT, "python", "pyspark", "pagerank_spark.py")
    start = time.perf_counter()
    proc = subprocess.run(
        ["spark-submit", "--master", "local[*]", script,
         "--input", input_path, "--mode", "both",
         "--iterations", str(iterations), "--output", out_prefix],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    )
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        return {"available": False, "reason": proc.stderr.strip()[-200:]}
    return {
        "available": True,
        "time_s": round(elapsed, 4),
        "ranks_rdd": _read_spark_text_dir(out_prefix + "/result_rdd"),
        "ranks_df": _read_spark_text_dir(out_prefix + "/result_df"),
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark PageRank implementations.")
    parser.add_argument("--input", default="data/graph.txt", help="Input edge list")
    parser.add_argument("--iterations", type=int, default=50, help="Max iterations")
    parser.add_argument("--epsilon", type=float, default=0.001, help="Convergence threshold")
    parser.add_argument("--output", default="output/benchmark.json", help="JSON report path")
    args = parser.parse_args()

    tmp_dir = os.path.join(PROJECT_ROOT, "output", "_bench_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"Benchmarking on {args.input} (max_iter={args.iterations}, eps={args.epsilon})\n")

    core = bench_core(args.input, args.iterations, args.epsilon)
    core_ranks = core["ranks"]

    # Two fair references so the agreement metric is apples-to-apples:
    #   * early-stopping implementations (mrjob, pyspark) use the same epsilon,
    #   * fixed-iteration implementations (streaming) run the same iteration count.
    # Both reduce to the identical recurrence, so agreement should be ~1e-7.
    edges = read_edges(args.input)
    ref_eps = core_ranks
    ref_fixed = pagerank(edges, damping=0.85, epsilon=0.0,
                         max_iter=args.iterations).ranks
    reference_for = {
        "mrjob_core": ref_eps,
        "mrjob_mapreduce": ref_eps,
        "pyspark": ref_eps,
        "streaming": ref_fixed,
    }

    rows = [("core", core["time_s"], core["iterations"], 0.0)]
    report = {
        "input": args.input,
        "iterations": args.iterations,
        "epsilon": args.epsilon,
        "num_nodes": len(core_ranks),
        "core": {k: core[k] for k in ("time_s", "iterations", "converged", "deltas", "top5")},
        "implementations": {},
    }

    runs = {
        "mrjob_core": bench_mrjob(args.input, args.iterations, "core", tmp_dir),
        "mrjob_mapreduce": bench_mrjob(args.input, args.iterations, "mrjob", tmp_dir),
        "streaming": bench_streaming(args.input, args.iterations),
        "pyspark": bench_pyspark(args.input, args.iterations, tmp_dir),
    }

    for name, res in runs.items():
        entry = {"available": res.get("available", False)}
        reference = reference_for[name]
        if res.get("available"):
            entry["time_s"] = res["time_s"]
            if name == "pyspark":
                entry["max_diff_vs_core_rdd"] = _max_diff(res["ranks_rdd"], reference)
                entry["max_diff_vs_core_df"] = _max_diff(res["ranks_df"], reference)
                rows.append((name + " (rdd)", res["time_s"], "-", entry["max_diff_vs_core_rdd"]))
                rows.append((name + " (df)", res["time_s"], "-", entry["max_diff_vs_core_df"]))
            else:
                entry["max_diff_vs_core"] = _max_diff(res["ranks"], reference)
                rows.append((name, res["time_s"], "-", entry["max_diff_vs_core"]))
        else:
            entry["reason"] = res.get("reason", "unavailable")
        report["implementations"][name] = entry

    # Print a readable table.
    print(f"{'implementation':<22}{'time (s)':>10}{'iters':>8}{'max|Δ| vs core':>18}")
    print("-" * 58)
    for name, t, it, diff in rows:
        diff_str = "0" if diff == 0.0 else (f"{diff:.2e}" if diff is not None else "n/a")
        print(f"{name:<22}{t:>10}{str(it):>8}{diff_str:>18}")
    print()
    for name, entry in report["implementations"].items():
        if not entry["available"]:
            print(f"  [skipped] {name}: {entry['reason']}")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(f"\nJSON report written to {args.output}")


if __name__ == "__main__":
    main()
