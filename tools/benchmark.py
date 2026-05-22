"""
Comparative benchmark for the PageRank implementations (scaling study).

For each graph size in ``config.GRAPH_SIZES`` and each available framework, this
script measures, by actually running the implementation as a subprocess:

    * wall-clock time           (time.perf_counter around the process)
    * peak resident memory      (/usr/bin/time -v "Maximum resident set size")
    * iterations to convergence (parsed from the program output)
    * accuracy                  (max per-node |Δ| vs the reference engine)

Results are appended to a CSV (``config.RESULTS_CSV``) consumed by visualize.py.
Frameworks whose runtime is missing (e.g. spark-submit on a laptop) are skipped
automatically, so the same command works locally and on a cluster. All numbers
are measured at run time — none are hard-coded.

Usage:
    python tools/benchmark.py --generate                  # all sizes, all frameworks
    python tools/benchmark.py --sizes 1000 10000 --frameworks core pyspark_rdd
"""

import argparse
import csv
import functools
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: E402  (tools/config.py)

sys.path.insert(0, os.path.join(config.PROJECT_ROOT, "python"))
from core import pagerank, read_edges  # noqa: E402

CSV_FIELDS = [
    "framework", "label", "n_nodes", "n_edges",
    "time_s", "peak_mem_mb", "iterations", "max_diff_vs_core", "status",
]


# ---------------------------------------------------------------------------
# Output readers
# ---------------------------------------------------------------------------
def _load_ranks(path):
    ranks = {}
    if not os.path.isfile(path):
        return ranks
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("node"):
                continue
            node, rank = line.split("\t")
            ranks[node] = float(rank)
    return ranks


def _read_spark_text_dir(directory):
    ranks = {}
    if not os.path.isdir(directory):
        return ranks
    for name in os.listdir(directory):
        if name.startswith("_") or name.endswith(".crc"):
            continue
        with open(os.path.join(directory, name), encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    node, rank = line.split("\t")
                    ranks[node] = float(rank)
    return ranks


def _max_diff(a, b):
    if not a or set(a) != set(b):
        return None
    return max(abs(a[n] - b[n]) for n in a)


# ---------------------------------------------------------------------------
# Subprocess timing + peak-memory measurement
# ---------------------------------------------------------------------------
_GNU_TIME = "/usr/bin/time" if os.path.exists("/usr/bin/time") else None


def _timed_run(cmd, env_extra=None):
    """Run ``cmd`` (list) and return (proc, elapsed_s, peak_mem_mb_or_None)."""
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)

    timefile = None
    wrapped = cmd
    if _GNU_TIME:
        fd, timefile = tempfile.mkstemp(prefix="bench_time_")
        os.close(fd)
        wrapped = [_GNU_TIME, "-v", "-o", timefile, *cmd]

    start = time.perf_counter()
    proc = subprocess.run(wrapped, env=env, cwd=config.PROJECT_ROOT,
                          capture_output=True, text=True)
    elapsed = time.perf_counter() - start

    peak_mb = None
    if timefile and os.path.exists(timefile):
        with open(timefile, encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if "Maximum resident set size" in line:
                    peak_mb = round(int(line.rsplit(":", 1)[1].strip()) / 1024.0, 1)
        os.remove(timefile)
    return proc, round(elapsed, 4), peak_mb


def _parse_iterations(stdout, max_iter):
    """Iterations actually run: the reported convergence point, else the full budget."""
    m = re.search(r"Converged after (\d+) iterations", stdout)
    return int(m.group(1)) if m else max_iter


# ---------------------------------------------------------------------------
# Framework command specs
# ---------------------------------------------------------------------------
def _spec(framework, graph_abs, out_prefix, iters, epsilon):
    """Return (cmd, env_extra, reader, ref_kind) for a framework.

    ``epsilon`` is forwarded to the early-stopping implementations; passing 0
    makes every framework run the same fixed ``iters`` for a fair time/memory
    comparison. ``ref_kind`` selects the in-process reference for the accuracy
    metric: "main" (same epsilon as the run) or "fixed" (exact iteration count,
    used for the no-early-stop Streaming pipeline).
    """
    eps = str(epsilon)
    mr = "python/mrjob/pagerank_mrjob.py"
    spark = "python/pyspark/pagerank_spark.py"
    if framework == "core":
        out = out_prefix + "_core.txt"
        return ([sys.executable, "-m", "core.cli", graph_abs,
                 "--iterations", str(iters), "--epsilon", eps, "--output", out],
                {"PYTHONPATH": os.path.join(config.PROJECT_ROOT, "python")},
                lambda: _load_ranks(out), "main")
    if framework == "mrjob_core":
        out = out_prefix + "_mrjob_core.txt"
        return ([sys.executable, mr, graph_abs, "--iterations", str(iters),
                 "--epsilon", eps, "--engine", "core", "--output", out],
                {}, lambda: _load_ranks(out), "main")
    if framework == "mrjob_mapreduce":
        out = out_prefix + "_mrjob_mr.txt"
        return ([sys.executable, mr, graph_abs, "--iterations", str(iters),
                 "--epsilon", eps, "--engine", "mrjob", "--output", out],
                {}, lambda: _load_ranks(out), "main")
    if framework == "streaming":
        out = os.path.join(config.OUTPUT_DIR, "pagerank_streaming.txt")
        return (["bash", "scripts/run_streaming.sh", str(iters), "--local", graph_abs],
                {}, lambda: _load_ranks(out), "fixed")
    if framework in ("pyspark_rdd", "pyspark_df"):
        mode = "rdd" if framework == "pyspark_rdd" else "df"
        pref = out_prefix + "_spark"
        result_dir = f"{pref}/result_{mode}"
        return (["spark-submit", "--master", "local[*]", spark,
                 "--input", graph_abs, "--mode", mode,
                 "--iterations", str(iters), "--epsilon", eps, "--output", pref],
                {}, functools.partial(_read_spark_text_dir, result_dir), "main")
    raise ValueError(f"unknown framework: {framework}")


def _available(framework):
    req = config.FRAMEWORKS[framework]["requires"]
    if req is None:
        return True
    if config.FRAMEWORKS[framework]["kind"] == "module":
        return importlib.util.find_spec(req) is not None
    return shutil.which(req) is not None


# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="PageRank comparative benchmark.")
    parser.add_argument("--sizes", type=int, nargs="*", help="Node counts (default: config)")
    parser.add_argument("--frameworks", nargs="*", help="Frameworks (default: all available)")
    parser.add_argument("--iterations", type=int, default=config.DEFAULT_ITERATIONS)
    parser.add_argument("--epsilon", type=float, default=0.0,
                        help="Convergence threshold passed to every framework. "
                             "Default 0 = run a fixed iteration count so all "
                             "frameworks do identical work (fair time comparison).")
    parser.add_argument("--generate", action="store_true",
                        help="Generate missing graphs before benchmarking")
    parser.add_argument("--output", default=config.RESULTS_CSV, help="Results CSV path")
    args = parser.parse_args()

    sizes = args.sizes if args.sizes else config.GRAPH_SIZES
    frameworks = args.frameworks if args.frameworks else config.DEFAULT_FRAMEWORKS
    iters = args.iterations

    runnable = [f for f in frameworks if _available(f)]
    skipped = [f for f in frameworks if not _available(f)]
    print(f"Sizes: {sizes}")
    print(f"Frameworks: {', '.join(runnable)}"
          + (f"   [skipped: {', '.join(skipped)}]" if skipped else ""))

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    tmp_dir = os.path.join(config.OUTPUT_DIR, "_bench_tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    rows = []
    for n in sizes:
        graph = config.graph_path_for(n)
        if not os.path.isfile(graph):
            if args.generate:
                from generate_graph import generate
                edges_gen = generate(n, config.BA_M, config.SEED, weighted=False)
                os.makedirs(os.path.dirname(graph), exist_ok=True)
                with open(graph, "w", encoding="utf-8") as fh:
                    fh.writelines(f"{s}\t{d}\n" for s, d in edges_gen)
                print(f"  generated {graph}")
            else:
                print(f"  [skip size {n}] missing {graph} (use --generate)")
                continue

        graph_abs = os.path.abspath(graph)
        edges = read_edges(graph)
        n_edges = len(edges)
        # In-process reference ranks for the accuracy metric (not benchmarked).
        # "main" matches the epsilon every framework runs with; "fixed" matches
        # the no-early-stop Streaming pipeline (exact iteration count).
        ref = {
            "main": pagerank(edges, damping=config.DAMPING,
                             epsilon=args.epsilon, max_iter=iters).ranks,
            "fixed": pagerank(edges, damping=config.DAMPING,
                              epsilon=0.0, max_iter=iters).ranks,
        }
        print(f"\n=== n={n} ({n_edges} edges) ===")

        for fw in runnable:
            label = config.FRAMEWORKS[fw]["label"]
            cmd, env_extra, reader, ref_kind = _spec(
                fw, graph_abs, os.path.join(tmp_dir, f"n{n}"), iters, args.epsilon)
            proc, elapsed, peak_mb = _timed_run(cmd, env_extra)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout).strip().splitlines()[-1:] or [""]
                print(f"  {label:<20} ERROR ({tail[0][:60]})")
                rows.append(dict(framework=fw, label=label, n_nodes=n, n_edges=n_edges,
                                 time_s="", peak_mem_mb="", iterations="",
                                 max_diff_vs_core="", status="error"))
                continue
            ranks = reader()
            diff = _max_diff(ranks, ref[ref_kind])
            iters_done = _parse_iterations(proc.stdout, iters)
            diff_str = "0" if diff == 0 else (f"{diff:.2e}" if diff is not None else "n/a")
            mem_str = f"{peak_mb}" if peak_mb is not None else "n/a"
            print(f"  {label:<20} {elapsed:>8.3f}s  {mem_str:>8} MB  "
                  f"iters={iters_done}  maxΔ={diff_str}")
            rows.append(dict(framework=fw, label=label, n_nodes=n, n_edges=n_edges,
                             time_s=elapsed, peak_mem_mb=peak_mb if peak_mb is not None else "",
                             iterations=iters_done,
                             max_diff_vs_core=diff if diff is not None else "",
                             status="ok"))

    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote {len(rows)} rows to {args.output}")
    if skipped:
        print(f"Note: {', '.join(skipped)} were skipped (runtime not installed); "
              f"run on a cluster to add those rows.")


if __name__ == "__main__":
    main()
