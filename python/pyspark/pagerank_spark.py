"""
PageRank implementation using PySpark.
Two approaches: RDD-based and DataFrame-based.

Both approaches implement the *full* PageRank recurrence, including correct
redistribution of dangling-node mass, so their results match the pure-Python
reference engine (python/core) and networkx.pagerank to ~1e-9:

    PR(u) = (1 - d)/N + d * dangling_mass / N + d * sum_{v->u} PR(v)/outdeg(v)

Usage:
  spark-submit pagerank_spark.py --input data/graph.txt --iterations 20
  spark-submit pagerank_spark.py --input hdfs:///graph.txt --iterations 20 --mode rdd
"""

import argparse
import os
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

DAMPING = 0.85
EPSILON = 0.001
MAX_ITER = 20


# =============================================================================
# RDD-BASED IMPLEMENTATION
# =============================================================================

def _compute_contribs(kv):
    """Emit (neighbor, share) for each out-link of a node.

    kv = (node, (neighbors, rank)). Each out-neighbour receives rank/out_degree.
    """
    _node, (neighbors, rank) = kv
    out_degree = len(neighbors)
    if out_degree == 0:
        return []
    share = rank / out_degree
    return [(nb, share) for nb in neighbors]


def pagerank_rdd(spark, input_path, iterations, output_path, epsilon=EPSILON):
    """Classic RDD-based PageRank.

    Educational and close to the raw MapReduce logic, but numerically complete:
    isolated/dangling nodes are tracked and their mass is redistributed.
    """
    sc = spark.sparkContext
    print(f"\n[PySpark RDD] Reading graph: {input_path}")

    # Parse edges: "src\tdst"
    edges = (sc.textFile(input_path)
             .filter(lambda line: line.strip() and "\t" in line)
             .map(lambda line: tuple(line.strip().split("\t")[:2]))
             .filter(lambda pair: len(pair) == 2)
             .cache())

    # All nodes, including dangling nodes that appear only as a destination.
    all_nodes = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()
    N = all_nodes.count()
    print(f"[PySpark RDD] Nodes: {N}, Damping: {DAMPING}")

    # Adjacency for EVERY node; dangling nodes map to an empty list.
    adj = edges.groupByKey().mapValues(list)
    links = (all_nodes.map(lambda n: (n, []))
             .leftOuterJoin(adj)
             .mapValues(lambda pair: pair[1] if pair[1] is not None else [])
             .cache())

    # Initialise ranks: 1/N for every node.
    ranks = all_nodes.map(lambda n: (n, 1.0 / N))

    teleport = (1.0 - DAMPING) / N

    for iteration in range(1, iterations + 1):
        # Mass held by dangling nodes (no out-links) — redistributed uniformly.
        dangling_mass = (links.filter(lambda kv: len(kv[1]) == 0)
                         .join(ranks)
                         .map(lambda kv: kv[1][1])
                         .sum())
        base = teleport + DAMPING * dangling_mass / N

        # Contributions from nodes that have out-links.
        contribs = (links.filter(lambda kv: len(kv[1]) > 0)
                    .join(ranks)
                    .flatMap(_compute_contribs)
                    .reduceByKey(add))

        # New rank for every node: base + damped incoming contributions.
        # ``base=base`` binds the current iteration's value into the closure;
        # Spark transformations are lazy, so capturing the loop variable by
        # reference would otherwise risk using a later iteration's base.
        new_ranks = (all_nodes.map(lambda n, base=base: (n, base))
                     .leftOuterJoin(contribs)
                     .mapValues(lambda pair: pair[0]
                                + DAMPING * (pair[1] if pair[1] is not None else 0.0)))

        # Truncate the RDD lineage each iteration; otherwise the DAG grows
        # unbounded and the driver can OOM while planning later iterations.
        # NOTE: RDD.localCheckpoint() marks the RDD in place and returns None
        # (unlike DataFrame.localCheckpoint), so it must NOT be reassigned.
        new_ranks = new_ranks.cache()
        new_ranks.localCheckpoint()

        # L1 convergence check (this action also materializes the checkpoint).
        delta = (ranks.join(new_ranks)
                 .map(lambda kv: abs(kv[1][0] - kv[1][1]))
                 .sum())

        ranks = new_ranks
        print(f"  Iteration {iteration:2d} | delta = {delta:.6f}")
        if delta < epsilon:
            print(f"  Converged after {iteration} iterations.")
            break

    # Sort by rank descending and save.
    result = ranks.sortBy(lambda kv: kv[1], ascending=False)
    result.map(lambda kv: f"{kv[0]}\t{kv[1]:.8f}").saveAsTextFile(output_path + "_rdd")

    print(f"\n[PySpark RDD] Results saved to: {output_path}_rdd")
    _print_top5(result.take(5))
    return result


# =============================================================================
# DATAFRAME-BASED IMPLEMENTATION
# =============================================================================

def pagerank_dataframe(spark, input_path, iterations, output_path, epsilon=EPSILON):
    """DataFrame-based PageRank.

    More scalable and Catalyst-optimised. Implements the same full recurrence as
    the RDD version (including dangling-mass redistribution).
    """
    print(f"\n[PySpark DataFrame] Reading graph: {input_path}")

    schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False),
    ])

    edges_df = (spark.read
                .option("sep", "\t")
                .option("header", False)
                .schema(schema)
                .csv(input_path)
                .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
                .cache())

    # Out-degree per source node; edge weight = 1 / out_degree.
    out_degree = edges_df.groupBy("src").agg(F.count("dst").alias("out_degree"))
    edges_weighted = (edges_df
                      .join(out_degree, on="src")
                      .withColumn("weight", F.lit(1.0) / F.col("out_degree"))
                      .select("src", "dst", "weight")
                      .cache())

    # All nodes (src or dst).
    all_nodes = (edges_df.select(F.col("src").alias("node"))
                 .union(edges_df.select(F.col("dst").alias("node")))
                 .distinct()
                 .cache())
    N = all_nodes.count()
    print(f"[PySpark DataFrame] Nodes: {N}, Damping: {DAMPING}")

    # Nodes WITHOUT out-links are dangling (present in all_nodes but not in out_degree).
    dangling_nodes = all_nodes.join(
        out_degree, all_nodes["node"] == out_degree["src"], how="left_anti"
    ).cache()

    ranks_df = all_nodes.withColumn("rank", F.lit(1.0 / N))
    teleport = (1.0 - DAMPING) / N

    for iteration in range(1, iterations + 1):
        # Dangling mass = total rank held by dangling nodes (scalar).
        dangling_mass = (dangling_nodes.join(ranks_df, on="node")
                         .agg(F.sum("rank")).collect()[0][0]) or 0.0
        base = teleport + DAMPING * dangling_mass / N

        # Contributions: each dst receives weight * rank(src).
        contribs = (edges_weighted
                    .join(ranks_df.withColumnRenamed("node", "src")
                                  .withColumnRenamed("rank", "src_rank"), on="src")
                    .withColumn("contrib", F.col("weight") * F.col("src_rank"))
                    .groupBy("dst")
                    .agg(F.sum("contrib").alias("rank_sum"))
                    .withColumnRenamed("dst", "node"))

        new_ranks = (all_nodes
                     .join(contribs, on="node", how="left")
                     .fillna(0.0, subset=["rank_sum"])
                     .withColumn("rank",
                                 F.lit(base) + F.lit(DAMPING) * F.col("rank_sum"))
                     .select("node", "rank"))
        # Truncate the logical plan each iteration. Otherwise the chained joins
        # build an ever-deeper plan that both explodes driver memory (an
        # OutOfMemoryError while Spark renders the plan tree / runs AQE) and makes
        # every iteration exponentially slower. localCheckpoint materializes the
        # result and cuts the lineage without needing an HDFS checkpoint dir.
        new_ranks = new_ranks.localCheckpoint(eager=True)

        delta = (ranks_df.withColumnRenamed("rank", "rank_old")
                 .join(new_ranks.withColumnRenamed("rank", "rank_new"), on="node")
                 .select(F.abs(F.col("rank_old") - F.col("rank_new")).alias("diff"))
                 .agg(F.sum("diff"))
                 .collect()[0][0])

        ranks_df = new_ranks
        print(f"  Iteration {iteration:2d} | delta = {delta:.6f}")
        if delta < epsilon:
            print(f"  Converged after {iteration} iterations.")
            break

    result = ranks_df.orderBy(F.col("rank").desc())
    result.write.mode("overwrite").option("sep", "\t").csv(output_path + "_df")

    print(f"\n[PySpark DataFrame] Results saved to: {output_path}_df")
    top5 = result.limit(5).collect()
    _print_top5([(row["node"], row["rank"]) for row in top5])
    return result


def _print_top5(top5):
    print("\nTop-5 nodes by PageRank:")
    print(f"{'Node':<10} {'PageRank':<12}")
    print("-" * 22)
    for node, rank in top5:
        print(f"{node:<10} {rank:.8f}")


# =============================================================================
# MAIN
# =============================================================================

def to_uri(path):
    """Resolve a scheme-less path to an absolute local ``file://`` URI.

    Spark resolves relative paths against Hadoop's ``fs.defaultFS``, which may be
    HDFS (e.g. ``hdfs://localhost:9000``). Forcing ``file://`` for scheme-less
    paths makes local input/output work regardless of the cluster's default
    filesystem. Explicit schemes (``hdfs://``, ``file://``, ``s3://`` ...) pass
    through unchanged.
    """
    if "://" in path:
        return path
    return "file://" + os.path.abspath(path)


def main():
    parser = argparse.ArgumentParser(description="PySpark PageRank")
    parser.add_argument("--input", required=True, help="Input edge list (TSV)")
    parser.add_argument("--output", default="output/pagerank_spark",
                        help="Output path prefix")
    parser.add_argument("--iterations", type=int, default=MAX_ITER)
    parser.add_argument("--epsilon", type=float, default=EPSILON,
                        help="L1 convergence threshold (pass 0 for a fixed iteration count)")
    parser.add_argument("--mode", choices=["rdd", "df", "both"], default="both",
                        help="Execution mode: rdd, df, or both")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("PageRank")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    input_uri = to_uri(args.input)
    output_uri = to_uri(args.output)
    if "://" not in args.output:           # create the local output dir only
        os.makedirs(args.output, exist_ok=True)

    if args.mode in ("rdd", "both"):
        pagerank_rdd(spark, input_uri, args.iterations, output_uri + "/result",
                     epsilon=args.epsilon)

    if args.mode in ("df", "both"):
        pagerank_dataframe(spark, input_uri, args.iterations, output_uri + "/result",
                           epsilon=args.epsilon)

    spark.stop()


if __name__ == "__main__":
    main()
