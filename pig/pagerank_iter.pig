-- =============================================================================
-- PageRank in Apache Pig Latin
-- =============================================================================
--
-- Usage (called by run_pig.sh with parameter substitution):
--   pig -param INPUT=hdfs:///pagerank/input/graph.txt       \
--       -param ITER_IN=hdfs:///pagerank/pig/iter_0          \
--       -param ITER_OUT=hdfs:///pagerank/pig/iter_1         \
--       -param N=10                                          \
--       -param DAMPING=0.85                                  \
--       pagerank_iter.pig
--
-- The shell script (run_pig.sh) calls this repeatedly until convergence.
-- =============================================================================

-- ---- Load current state ----
-- Format: node_id \t rank \t neighbor1,neighbor2,...
-- (created by init.pig on first call, then this script's own output)

node_state = LOAD '$ITER_IN' AS (
    node:    chararray,
    rank:    double,
    neighbors: chararray
);

-- ---- Parse neighbor list ----
-- TOKENIZE splits on commas; FLATTEN creates one row per neighbor
node_with_neighbors = FOREACH node_state GENERATE
    node,
    rank,
    neighbors,
    (IsEmpty(neighbors) ? 0 : SIZE(TOKENIZE(neighbors, ','))) AS out_degree: int;

-- ---- Compute contributions ----
-- For each node, emit (neighbor, rank / out_degree) for every out-neighbor

contributions_raw = FOREACH node_with_neighbors {
    nb_bag = TOKENIZE(neighbors, ',');
    GENERATE
        FLATTEN(nb_bag) AS dst: chararray,
        (out_degree > 0 ? rank / (double)out_degree : 0.0) AS contrib: double,
        node,
        neighbors,
        out_degree;
}

-- Keep only valid contributions (out_degree > 0)
contributions = FILTER contributions_raw BY out_degree > 0;

-- ---- Aggregate contributions per destination ----
grouped_contribs = GROUP contributions BY dst;

rank_sums = FOREACH grouped_contribs GENERATE
    group                    AS node: chararray,
    SUM(contributions.contrib) AS rank_sum: double;

-- ---- Apply PageRank formula over the FULL node set ----
-- new_rank = (1 - d) / N + d * rank_sum,  where d = damping, N = total nodes.
-- A LEFT OUTER JOIN from every node onto rank_sums guarantees that nodes which
-- received no incoming contribution this iteration are still emitted (they get
-- the teleportation term only) and that each node keeps its adjacency list for
-- the next iteration.
all_nodes = FOREACH node_state GENERATE node, neighbors;

all_with_rank = JOIN all_nodes BY node LEFT OUTER, rank_sums BY node;

new_ranks = FOREACH all_with_rank GENERATE
    all_nodes::node     AS node:      chararray,
    (double)((1.0 - $DAMPING) / $N
        + $DAMPING * (rank_sums::rank_sum IS NULL ? 0.0 : rank_sums::rank_sum))
                         AS rank:      double,
    all_nodes::neighbors AS neighbors: chararray;

-- ---- Store output ----
STORE new_ranks INTO '$ITER_OUT'
    USING PigStorage('\t');
