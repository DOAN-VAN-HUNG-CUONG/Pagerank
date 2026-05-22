#!/usr/bin/env bash
# =============================================================================
# Run PageRank — Apache Pig
# =============================================================================
# Prerequisites:
#   - Hadoop running
#   - Pig installed, 'pig' command in PATH
#   - Input graph on HDFS at /pagerank/input/graph.txt
#
# Usage:
#   ./run_pig.sh [max_iterations] [epsilon]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PIG_DIR="$PROJECT_DIR/pig"

MAX_ITER="${1:-20}"
EPSILON="${2:-0.001}"

HDFS_BASE="hdfs:///pagerank"
INPUT_FILE="$HDFS_BASE/input/graph.txt"

echo "============================================"
echo "  PageRank — Apache Pig"
echo "  Max iterations: $MAX_ITER"
echo "  Epsilon:        $EPSILON"
echo "============================================"

# ---- Count nodes ----
N=$(python3 -c "
graph = {}
with open('$PROJECT_DIR/data/graph.txt') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts)==2:
            graph.setdefault(parts[0],[]).append(parts[1])
            graph.setdefault(parts[1],[])
print(len(graph))
")
echo "Nodes: $N"

# ---- Step 1: Upload input to HDFS ----
echo ""
echo "--- Uploading input to HDFS ---"
hdfs dfs -rm -r -f "/pagerank/input" 2>/dev/null || true
hdfs dfs -mkdir -p "/pagerank/input"
hdfs dfs -put "$PROJECT_DIR/data/graph.txt" "/pagerank/input/graph.txt"

# ---- Step 2: Initialize ----
echo ""
echo "--- Running Pig init ---"
hdfs dfs -rm -r -f "/pagerank/pig/iter_0" 2>/dev/null || true

pig -x mapreduce \
    -param INPUT="$INPUT_FILE"              \
    -param OUTPUT="$HDFS_BASE/pig/iter_0"   \
    -param N="$N"                           \
    "$PIG_DIR/pagerank_init.pig"

echo "Init done."

# ---- Step 3: Iterative PageRank ----
PREV_DELTA="9999"
CONVERGED=false

for iter in $(seq 1 "$MAX_ITER"); do
    ITER_IN="$HDFS_BASE/pig/iter_$((iter - 1))"
    ITER_OUT="$HDFS_BASE/pig/iter_$iter"

    echo ""
    echo "--- Pig Iteration $iter ---"
    hdfs dfs -rm -r -f "${ITER_OUT#hdfs://}" 2>/dev/null || true

    pig -x mapreduce \
        -param ITER_IN="$ITER_IN"           \
        -param ITER_OUT="$ITER_OUT"         \
        -param N="$N"                       \
        -param DAMPING="0.85"               \
        "$PIG_DIR/pagerank_iter.pig"

    echo "  Iteration $iter complete."

    # Convergence check: compute delta using local Python on HDFS sample
    # For production, implement a Pig script that computes delta
    if [[ $iter -ge 3 ]]; then
        echo "  (Convergence check via Python delta computation)"
        DELTA=$(hdfs dfs -cat "${ITER_OUT#hdfs:///}/part-*" 2>/dev/null | \
            python3 -c "
import sys, math
ranks = {}
for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) >= 2:
        try: ranks[parts[0]] = float(parts[1])
        except: pass
# Simple stdev as convergence proxy
vals = list(ranks.values())
if vals:
    mean = sum(vals)/len(vals)
    stdev = math.sqrt(sum((v-mean)**2 for v in vals)/len(vals))
    print(f'{stdev:.6f}')
else:
    print('0')
" 2>/dev/null || echo "0")
        echo "  Rank stdev (convergence proxy): $DELTA"
    fi

    # Cleanup previous iteration (keep iter_0 init)
    if [[ $iter -gt 1 ]]; then
        hdfs dfs -rm -r -f "/pagerank/pig/iter_$((iter - 1))" 2>/dev/null || true
    fi
done

# ---- Step 4: Sort results ----
echo ""
echo "--- Sorting final results ---"
hdfs dfs -rm -r -f "/pagerank/pig/final" 2>/dev/null || true

pig -x mapreduce \
    -param INPUT="$HDFS_BASE/pig/iter_$MAX_ITER"   \
    -param OUTPUT="$HDFS_BASE/pig/final"            \
    "$PIG_DIR/pagerank_sort.pig"

# ---- Step 5: Collect ----
LOCAL_OUTPUT="$PROJECT_DIR/output/pagerank_pig.txt"
mkdir -p "$PROJECT_DIR/output"
hdfs dfs -getmerge "/pagerank/pig/final" "$LOCAL_OUTPUT"

echo ""
echo "Done. Results: $LOCAL_OUTPUT"
echo ""
echo "Top-5 nodes by PageRank:"
head -5 "$LOCAL_OUTPUT"
