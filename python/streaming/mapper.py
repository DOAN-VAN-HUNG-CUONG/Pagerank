#!/usr/bin/env python3
"""
PageRank Mapper for Hadoop Streaming.

Input format (one record per line, tab-separated):
  node_id   rank|neighbor1,neighbor2,...

Example:
  1   0.1|2,3
  2   0.1|3,4

Output:
  neighbor_id   rank_share          -- rank contribution to neighbor
  node_id       GRAPH|neighbors     -- preserve graph structure

Usage with Hadoop Streaming:
  hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  hdfs:///pagerank/iter_N/   \
    -output hdfs:///pagerank/iter_N+1/ \
    -mapper  "python3 mapper.py"       \
    -reducer "python3 reducer.py"
"""

import sys


def mapper():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 2:
            continue

        node = parts[0]
        state = parts[1]

        # State format: "rank|neighbor1,neighbor2,..."
        if '|' not in state:
            continue

        rank_str, neighbors_str = state.split('|', 1)
        rank = float(rank_str)
        neighbors = neighbors_str.split(',') if neighbors_str else []

        # 1. Preserve graph structure for reducer
        yield node, f"GRAPH|{neighbors_str}"

        # 2. Distribute rank equally to each out-neighbor
        if neighbors:
            share = rank / len(neighbors)
            for nb in neighbors:
                yield nb, str(share)


if __name__ == '__main__':
    for key, value in mapper():
        print(f"{key}\t{value}")
