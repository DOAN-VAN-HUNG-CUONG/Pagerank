#!/usr/bin/env bash
# =============================================================================
# Run PageRank — Hadoop Streaming (Python mapper/reducer)
# =============================================================================
# Two modes:
#   * cluster (default): submits a streaming job per iteration to Hadoop.
#       Prerequisites: Hadoop running, HADOOP_HOME set, graph uploaded to HDFS.
#   * local  (--local):  simulates the exact pipeline with shell pipes
#       (init | mapper | sort | reducer), no Hadoop required. Useful for tests
#       and for verifying numerical agreement with the reference engine.
#
# Usage:
#   ./run_streaming.sh [max_iterations] [--epsilon E] [--local] [--input FILE]
#   ./run_streaming.sh 20 --local data/graph.txt
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

MAX_ITER=20
EPSILON=0.001
LOCAL=0
INPUT="$PROJECT_DIR/data/graph.txt"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)   LOCAL=1; shift ;;
        --input)   INPUT="$2"; shift 2 ;;
        --epsilon) EPSILON="$2"; shift 2 ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                MAX_ITER="$1"; shift
            elif [[ -f "$1" ]]; then
                INPUT="$1"; shift
            else
                echo "Unknown argument: $1"; exit 1
            fi ;;
    esac
done

MAPPER="$PROJECT_DIR/python/streaming/mapper.py"
REDUCER="$PROJECT_DIR/python/streaming/reducer.py"
INIT_SCRIPT="$PROJECT_DIR/python/streaming/init.py"

count_nodes() {
    python3 - "$INPUT" <<'PY'
import sys
graph = {}
with open(sys.argv[1]) as fh:
    for line in fh:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            graph.setdefault(parts[0], []).append(parts[1])
            graph.setdefault(parts[1], [])
print(len(graph))
PY
}

# -----------------------------------------------------------------------------
# Local simulation (no Hadoop)
# -----------------------------------------------------------------------------
run_local() {
    local N work prev cur out
    N="$(count_nodes)"
    echo "============================================"
    echo "  PageRank — Hadoop Streaming (LOCAL simulation)"
    echo "  Input: $INPUT | Nodes: $N | Max iter: $MAX_ITER"
    echo "============================================"

    work="$(mktemp -d)"
    python3 "$INIT_SCRIPT" "$INPUT" > "$work/state_0" 2>/dev/null
    prev="$work/state_0"

    for ((i = 1; i <= MAX_ITER; i++)); do
        cur="$work/state_$i"
        # mapper | shuffle (sort by key) | reducer  — mirrors the MR data flow.
        python3 "$MAPPER" < "$prev" \
            | sort -t$'\t' -k1,1 \
            | PAGERANK_N="$N" PAGERANK_DAMPING=0.85 python3 "$REDUCER" > "$cur"
        echo "  Iteration $i complete."
        prev="$cur"
    done

    out="$PROJECT_DIR/output/pagerank_streaming.txt"
    mkdir -p "$PROJECT_DIR/output"
    python3 - "$prev" "$out" <<'PY'
import sys
rows = []
with open(sys.argv[1]) as fh:
    for line in fh:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        node, state = line.split('\t', 1)
        rank = float(state.split('|', 1)[0])
        rows.append((node, rank))
rows.sort(key=lambda kv: (-kv[1], kv[0]))
with open(sys.argv[2], 'w') as out:
    out.write("node\tpagerank\n")
    for node, rank in rows:
        out.write(f"{node}\t{rank:.8f}\n")
print(f"  Sum of ranks (sanity): {sum(r for _, r in rows):.8f}")
PY
    echo ""
    echo "Done (local). Results: $out"
    head -6 "$out"
}

# -----------------------------------------------------------------------------
# Cluster execution (Hadoop Streaming)
# -----------------------------------------------------------------------------
run_cluster() {
    local HDFS_BASE="/pagerank" STREAMING_JAR N LOCAL_INIT FINAL_LOCAL
    STREAMING_JAR="$(find "${HADOOP_HOME:-/opt/hadoop}/share/hadoop/tools/lib" \
        -name 'hadoop-streaming-*.jar' 2>/dev/null | head -1)"
    if [[ -z "$STREAMING_JAR" ]]; then
        echo "ERROR: hadoop-streaming jar not found under HADOOP_HOME."
        echo "Tip: run with --local to simulate the pipeline without Hadoop."
        exit 1
    fi

    N="$(count_nodes)"
    echo "============================================"
    echo "  PageRank — Hadoop Streaming (cluster)"
    echo "  Input: $INPUT | Nodes: $N | Max iter: $MAX_ITER"
    echo "  Streaming jar: $STREAMING_JAR"
    echo "============================================"

    LOCAL_INIT="/tmp/pagerank_init.txt"
    python3 "$INIT_SCRIPT" "$INPUT" > "$LOCAL_INIT"

    hdfs dfs -rm -r -f "$HDFS_BASE/iter_0" 2>/dev/null || true
    hdfs dfs -mkdir -p "$HDFS_BASE/iter_0"
    hdfs dfs -put "$LOCAL_INIT" "$HDFS_BASE/iter_0/part-00000"

    for iter in $(seq 1 "$MAX_ITER"); do
        local ITER_IN="$HDFS_BASE/iter_$((iter - 1))"
        local ITER_OUT="$HDFS_BASE/iter_$iter"
        echo ""
        echo "--- Iteration $iter ---"
        hdfs dfs -rm -r -f "$ITER_OUT" 2>/dev/null || true
        hadoop jar "$STREAMING_JAR" \
            -input  "$ITER_IN"  \
            -output "$ITER_OUT" \
            -mapper  "python3 mapper.py"  \
            -reducer "python3 reducer.py" \
            -file    "$MAPPER"  \
            -file    "$REDUCER" \
            -cmdenv  "PAGERANK_N=$N" \
            -cmdenv  "PAGERANK_DAMPING=0.85"
        echo "  Iteration $iter complete."
    done

    FINAL_LOCAL="$PROJECT_DIR/output/pagerank_streaming.txt"
    mkdir -p "$PROJECT_DIR/output"
    hdfs dfs -getmerge "$HDFS_BASE/iter_$MAX_ITER" "$FINAL_LOCAL"
    echo ""
    echo "Done. Raw state: $FINAL_LOCAL (sort by rank for ranking)."
}

if [[ "$LOCAL" -eq 1 ]]; then
    run_local
else
    run_cluster
fi
