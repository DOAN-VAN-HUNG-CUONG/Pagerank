#!/usr/bin/env bash
# =============================================================================
# Build and Run PageRank — Java Hadoop MapReduce
# =============================================================================
# Prerequisites:
#   - Java 8+, Maven installed
#   - Hadoop running ($HADOOP_HOME set)
#   - graph.txt in data/
#
# Usage:
#   ./run_java.sh [max_iterations]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAVA_DIR="$PROJECT_DIR/java"

MAX_ITER="${1:-20}"

echo "============================================"
echo "  PageRank — Java Hadoop MapReduce"
echo "  Max iterations: $MAX_ITER"
echo "============================================"

# ---- Build ----
echo ""
echo "--- Building JAR ---"
cd "$JAVA_DIR"
mvn clean package -q
JAR="$JAVA_DIR/target/pagerank.jar"
echo "  Built: $JAR"
cd "$PROJECT_DIR"

# ---- Upload to HDFS ----
echo ""
echo "--- Uploading data to HDFS ---"
hdfs dfs -rm -r -f "/pagerank/java_input"  2>/dev/null || true
hdfs dfs -rm -r -f "/pagerank/java_output" 2>/dev/null || true
hdfs dfs -mkdir -p "/pagerank/java_input"
hdfs dfs -put "$PROJECT_DIR/data/graph.txt" "/pagerank/java_input/graph.txt"

# ---- Run ----
echo ""
echo "--- Running PageRank ---"
hadoop jar "$JAR" pagerank.PageRankDriver \
    -input      "hdfs:///pagerank/java_input"   \
    -output     "hdfs:///pagerank/java_output"  \
    -iterations "$MAX_ITER"

# ---- Collect results ----
LOCAL_OUTPUT="$PROJECT_DIR/output/pagerank_java.txt"
mkdir -p "$PROJECT_DIR/output"
hdfs dfs -getmerge "/pagerank/java_output/final" "$LOCAL_OUTPUT"

echo ""
echo "Done. Results: $LOCAL_OUTPUT"
echo ""
echo "Top-5 nodes by PageRank:"
head -5 "$LOCAL_OUTPUT"
