#!/usr/bin/env python3
"""
Init script for Hadoop Streaming PageRank.

Converts raw edge list to initial PageRank state:
  Input:  "src\tdst"
  Output: "node\t0.1|neighbor1,neighbor2,..."

Usage:
  python3 init.py data/graph.txt > output/init_state.txt

  # Then upload to HDFS:
  hdfs dfs -put output/init_state.txt /pagerank/iter_0/part-00000

  # Run iterations with streaming/run.sh
"""

import sys
from collections import defaultdict


def init(input_file):
    graph = defaultdict(list)
    all_nodes = set()

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) != 2:
                continue
            src, dst = parts[0], parts[1]
            graph[src].append(dst)
            all_nodes.add(src)
            all_nodes.add(dst)

    # Dangling nodes: appear as dst but have no outlinks
    for node in all_nodes:
        if node not in graph:
            graph[node] = []

    N = len(all_nodes)
    init_rank = 1.0 / N

    for node in sorted(graph.keys()):
        neighbors_str = ','.join(graph[node])
        print(f"{node}\t{init_rank:.8f}|{neighbors_str}")

    print(f"# Nodes: {N}", file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <edge_list_file>", file=sys.stderr)
        sys.exit(1)
    init(sys.argv[1])
