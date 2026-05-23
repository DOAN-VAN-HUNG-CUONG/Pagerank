#!/usr/bin/env bash
# =============================================================================
# Full comparative benchmark for the PageRank study (one command).
#
# Runs every framework whose runtime is installed -- including PySpark (RDD and
# DataFrame) -- in ONE consistent environment, then regenerates the paper
# figures. Frameworks whose runtime is absent (e.g. spark-submit on a laptop)
# are auto-skipped by tools/benchmark.py, so the same command works everywhere.
#
# Run this on a machine that has spark-submit on PATH (e.g. your OrbStack VM)
# to fill in the PySpark rows the sandbox could not measure. Because all rows
# are produced in a single environment, the resulting CSV is internally
# consistent -- do not mix it with numbers measured elsewhere.
#
# Usage:
#     bash scripts/run_full_benchmark.sh
#
# Tunable via environment variables (defaults shown):
#     SIZES="1000 5000 10000 50000"   node counts to benchmark
#     REPEAT=3                        timed runs per (framework, size)
#     ITERS=30                        PageRank iterations (fixed, for fair timing)
#     FRAMEWORKS="core mrjob_core mrjob_mapreduce streaming pyspark_rdd pyspark_df"
#     OUTPUT=output/benchmark_results.csv
#     FIGDIR=docs/figures
#     PYTHON=python3
#
# Example (PySpark only, larger graphs, 5 repeats):
#     FRAMEWORKS="pyspark_rdd pyspark_df" SIZES="10000 50000 100000" REPEAT=5 \
#         bash scripts/run_full_benchmark.sh
#
# NOTE on Java MapReduce and Apache Pig: these target a Hadoop cluster and are
# NOT part of the timing harness (they need HDFS/YARN). Build and run them
# separately as documented in README sections 5.5 (Java) and 5.6 (Pig); their
# numerical correctness is already covered by the test suite.
# =============================================================================
set -euo pipefail

# Resolve project root (parent of this script's directory) and cd into it.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

SIZES="${SIZES:-1000 5000 10000 50000}"
REPEAT="${REPEAT:-3}"
ITERS="${ITERS:-30}"
FRAMEWORKS="${FRAMEWORKS:-core mrjob_core mrjob_mapreduce streaming pyspark_rdd pyspark_df}"
OUTPUT="${OUTPUT:-output/benchmark_results.csv}"
FIGDIR="${FIGDIR:-docs/figures}"
PYTHON="${PYTHON:-python3}"

echo "=============================================================="
echo " Environment (record this for the paper's Methodology section)"
echo "=============================================================="
uname -srm || true
"$PYTHON" --version
echo "CPUs: $(nproc 2>/dev/null || echo '?')"
(free -h 2>/dev/null | awk 'NR<=2{print}') || true
if command -v spark-submit >/dev/null 2>&1; then
    echo "spark-submit: $(spark-submit --version 2>&1 | grep -m1 -i version || echo present)"
else
    echo "spark-submit: NOT found -> PySpark rows will be auto-skipped."
fi
echo

echo "=============================================================="
echo " Benchmark: sizes=[$SIZES] repeat=$REPEAT iters=$ITERS"
echo " frameworks: $FRAMEWORKS"
echo "=============================================================="
# shellcheck disable=SC2086  # word-splitting of SIZES/FRAMEWORKS is intended
"$PYTHON" tools/benchmark.py --generate \
    --sizes $SIZES \
    --frameworks $FRAMEWORKS \
    --repeat "$REPEAT" \
    --iterations "$ITERS" \
    --epsilon 0 \
    --output "$OUTPUT"

echo
echo "=============================================================="
echo " Regenerating figures into $FIGDIR/"
echo "=============================================================="
"$PYTHON" tools/visualize.py --csv "$OUTPUT" --input data/graph.txt --outdir "$FIGDIR"

# Keep the paper's figure copy in sync when writing to the default location.
if [ "$FIGDIR" = "docs/figures" ] && [ -d paper/figures ]; then
    for f in runtime_scaling memory_scaling speedup accuracy convergence; do
        [ -f "docs/figures/$f.pdf" ] && cp "docs/figures/$f.pdf" "paper/figures/$f.pdf"
    done
    echo "Synced 5 figures to paper/figures/."
fi

echo
echo "Done. Results: $OUTPUT"
echo "Next steps:"
echo "  1. Update the Environment paragraph in paper/content.tex with the machine"
echo "     spec printed above (CPU/RAM/OS) and the new framework set."
echo "  2. Rebuild the paper:  make paper        (IEEEtran submission build)"
echo "                  or:    make paper-preview (no IEEEtran.cls needed)"
