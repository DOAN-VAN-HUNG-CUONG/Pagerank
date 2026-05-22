#!/usr/bin/env python3
"""
PageRank Reducer for Hadoop Streaming.

Input (sorted by key from mapper):
  node_id   GRAPH|neighbors       -- graph structure
  node_id   rank_share_value      -- rank contribution (numeric)

Output:
  node_id   new_rank|neighbors    -- updated state for next iteration

Environment variables (set in streaming job config):
  PAGERANK_N       : total number of nodes
  PAGERANK_DAMPING : damping factor (default 0.85)
"""

import os
import sys

DAMPING = float(os.environ.get('PAGERANK_DAMPING', '0.85'))
N = int(os.environ.get('PAGERANK_N', '1'))


def reducer():
    current_node = None
    neighbors = []
    rank_sum = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 2:
            continue

        node, value = parts

        # New node: flush previous
        if node != current_node:
            if current_node is not None:
                yield current_node, compute_rank(rank_sum, neighbors)
            current_node = node
            neighbors = []
            rank_sum = 0.0

        if value.startswith('GRAPH|'):
            # Restore adjacency list
            nb_str = value[6:]   # after "GRAPH|"
            neighbors = nb_str.split(',') if nb_str else []
        else:
            # Accumulate rank contribution
            try:
                rank_sum += float(value)
            except ValueError:
                pass

    # Flush last node
    if current_node is not None:
        yield current_node, compute_rank(rank_sum, neighbors)


def compute_rank(rank_sum, neighbors):
    """Apply PageRank formula and reformat state."""
    new_rank = (1.0 - DAMPING) / N + DAMPING * rank_sum
    nb_str = ','.join(neighbors)
    return f"{new_rank:.8f}|{nb_str}"


if __name__ == '__main__':
    for key, value in reducer():
        print(f"{key}\t{value}")
