#!/usr/bin/env bash
# =============================================================================
# Run PageRank — PySpark
# =============================================================================
# Usage:
#   ./run_pyspark.sh [mode] [input] [iterations]
#   mode: rdd | df | both (default: both)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MODE="${1:-both}"
INPUT="${2:-$PROJECT_DIR/data/graph.txt}"
ITERATIONS="${3:-20}"
OUTPUT="$PROJECT_DIR/output/pyspark"

mkdir -p "$OUTPUT"

echo "============================================"
echo "  PageRank — PySpark"
echo "  Mode:       $MODE"
echo "  Input:      $INPUT"
echo "  Iterations: $ITERATIONS"
echo "  Output:     $OUTPUT"
echo "============================================"

spark-submit \
    --master local[*] \
    --conf "spark.driver.memory=2g" \
    "$PROJECT_DIR/python/pyspark/pagerank_spark.py" \
    --input "$INPUT" \
    --output "$OUTPUT/result" \
    --iterations "$ITERATIONS" \
    --mode "$MODE"

echo ""
echo "Done."
echo "RDD output: $OUTPUT/result_rdd/"
echo " DF output: $OUTPUT/result_df/"
