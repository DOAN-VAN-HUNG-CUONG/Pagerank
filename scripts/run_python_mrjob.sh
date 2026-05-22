#!/usr/bin/env bash
# =============================================================================
# Run PageRank — Python (mrjob local simulation)
# =============================================================================
# Usage:
#   ./run_python_mrjob.sh [input_file] [iterations]
#
# Default: uses data/graph.txt, 20 iterations
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

INPUT="${1:-$PROJECT_DIR/data/graph.txt}"
ITERATIONS="${2:-20}"
OUTPUT="$PROJECT_DIR/output/pagerank_mrjob.txt"

mkdir -p "$PROJECT_DIR/output"

echo "============================================"
echo "  PageRank — Python mrjob (local)"
echo "  Input:      $INPUT"
echo "  Iterations: $ITERATIONS"
echo "  Output:     $OUTPUT"
echo "============================================"

python3 "$PROJECT_DIR/python/mrjob/pagerank_mrjob.py" \
    "$INPUT" \
    --iterations "$ITERATIONS" \
    --output "$OUTPUT"

echo ""
echo "Done. Output: $OUTPUT"
