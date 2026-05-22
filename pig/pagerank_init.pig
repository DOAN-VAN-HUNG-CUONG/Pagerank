-- =============================================================================
-- PageRank Init in Apache Pig Latin
-- =============================================================================
--
-- Converts raw edge list to initial PageRank state.
--
-- Usage:
--   pig -param INPUT=hdfs:///pagerank/input/graph.txt  \
--       -param OUTPUT=hdfs:///pagerank/pig/iter_0      \
--       -param N=10                                    \
--       pagerank_init.pig
-- =============================================================================

-- Load raw edges: "src \t dst"
edges = LOAD '$INPUT' AS (src: chararray, dst: chararray);

-- Remove blank/null records
edges_clean = FILTER edges BY src IS NOT NULL AND dst IS NOT NULL;

-- ---- Build adjacency list ----
-- Group all destinations by source
edges_grouped = GROUP edges_clean BY src;

adj_list = FOREACH edges_grouped GENERATE
    group                                  AS node:      chararray,
    BagToString(edges_clean.dst, ',')      AS neighbors: chararray;

-- ---- Collect all nodes (source AND destination) ----
-- This ensures dangling nodes (appear only as dst) are included
src_nodes = FOREACH edges_clean GENERATE src AS node;
dst_nodes = FOREACH edges_clean GENERATE dst AS node;

all_node_bags = UNION src_nodes, dst_nodes;
all_nodes = DISTINCT all_node_bags;

-- ---- Join to get adjacency list for every node ----
-- Dangling nodes get empty neighbor string
all_with_adj = JOIN all_nodes BY node LEFT OUTER, adj_list BY node;

-- ---- Assign initial rank = 1 / N ----
initial_state = FOREACH all_with_adj GENERATE
    all_nodes::node      AS node:      chararray,
    1.0 / (double)$N     AS rank:      double,
    (adj_list::neighbors IS NOT NULL ? adj_list::neighbors : '')
                          AS neighbors: chararray;

-- ---- Store ----
-- Format: node \t rank \t neighbors
STORE initial_state INTO '$OUTPUT'
    USING PigStorage('\t');
