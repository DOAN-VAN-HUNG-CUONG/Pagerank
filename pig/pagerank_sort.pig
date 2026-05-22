-- =============================================================================
-- Sort PageRank results (descending)
-- =============================================================================
--
-- Usage:
--   pig -param INPUT=hdfs:///pagerank/pig/iter_N  \
--       -param OUTPUT=hdfs:///pagerank/pig/final  \
--       pagerank_sort.pig
-- =============================================================================

-- Load final iteration state
final_state = LOAD '$INPUT' AS (
    node:      chararray,
    rank:      double,
    neighbors: chararray
);

-- Sort by rank descending
sorted = ORDER final_state BY rank DESC;

-- Output only node and rank (drop adjacency list)
result = FOREACH sorted GENERATE node, rank;

-- Store sorted results
STORE result INTO '$OUTPUT'
    USING PigStorage('\t');
