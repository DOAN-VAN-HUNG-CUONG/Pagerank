package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PageRank Driver — orchestrates the full MapReduce pipeline.
 *
 * Pipeline:
 *   1. GraphInitJob  : parse edge list → (node, rank|neighbors)
 *   2. PageRankJob   : iterative MapReduce until convergence
 *   3. SortRankJob   : sort by rank descending
 *
 * Usage:
 *   hadoop jar pagerank.jar pagerank.PageRankDriver \
 *     -input  hdfs:///pagerank/input    \
 *     -output hdfs:///pagerank/output   \
 *     -iterations 20
 */
public class PageRankDriver extends Configured implements Tool {

    public static final double DAMPING = 0.85;
    public static final double EPSILON = 0.001;
    public static final String CONF_NUM_NODES  = "pagerank.num.nodes";
    public static final String CONF_DAMPING    = "pagerank.damping";

    @Override
    public int run(String[] args) throws Exception {
        // Parse arguments
        String inputPath  = null;
        String outputPath = null;
        int    maxIter    = 20;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-input":      inputPath  = args[++i]; break;
                case "-output":     outputPath = args[++i]; break;
                case "-iterations": maxIter    = Integer.parseInt(args[++i]); break;
            }
        }

        if (inputPath == null || outputPath == null) {
            System.err.println("Usage: PageRankDriver -input <path> -output <path> [-iterations <n>]");
            return 1;
        }

        Configuration conf = getConf();
        conf.setDouble(CONF_DAMPING, DAMPING);
        FileSystem fs = FileSystem.get(conf);

        // ----------------------------------------------------------------
        // Step 1: Initialize graph
        //   Input:  raw edge list ("src\tdst")
        //   Output: "node\trank|neighbor1,neighbor2,..."
        // ----------------------------------------------------------------
        String initOutput = outputPath + "/init";
        System.out.println("=== Step 1: Graph initialization ===");

        Job initJob = GraphInitJob.createJob(conf, inputPath, initOutput);
        if (!initJob.waitForCompletion(true)) {
            System.err.println("Graph init job failed.");
            return 1;
        }

        // Retrieve total node count from counter
        long numNodes = initJob.getCounters()
            .findCounter(GraphInitJob.Counters.NUM_NODES)
            .getValue();
        System.out.printf("Total nodes: %d%n", numNodes);
        conf.setLong(CONF_NUM_NODES, numNodes);

        // ----------------------------------------------------------------
        // Step 2: Iterative PageRank
        // ----------------------------------------------------------------
        String currentInput = initOutput;
        double lastDelta    = Double.MAX_VALUE;
        int    iteration    = 0;

        for (iteration = 1; iteration <= maxIter; iteration++) {
            String iterOutput = outputPath + "/iter_" + iteration;
            System.out.printf("%n=== Iteration %d ===%n", iteration);

            Job prJob = PageRankIterJob.createJob(conf, currentInput, iterOutput);
            if (!prJob.waitForCompletion(true)) {
                System.err.printf("PageRank iteration %d failed.%n", iteration);
                return 1;
            }

            // Read delta from counter (stored as long with 8-decimal precision)
            long deltaLong = prJob.getCounters()
                .findCounter(PageRankIterJob.Counters.DELTA_SUM)
                .getValue();
            lastDelta = deltaLong / 1e8;

            System.out.printf("  delta = %.6f%n", lastDelta);

            // Cleanup previous iteration output (except init)
            if (iteration > 1) {
                fs.delete(new Path(currentInput), true);
            }
            currentInput = iterOutput;

            if (lastDelta < EPSILON) {
                System.out.printf("  Converged after %d iterations.%n", iteration);
                break;
            }
        }

        // ----------------------------------------------------------------
        // Step 3: Sort by rank descending
        // ----------------------------------------------------------------
        String finalOutput = outputPath + "/final";
        System.out.println("\n=== Step 3: Sorting by rank ===");

        Job sortJob = SortRankJob.createJob(conf, currentInput, finalOutput);
        if (!sortJob.waitForCompletion(true)) {
            System.err.println("Sort job failed.");
            return 1;
        }

        System.out.println("\n=== Done ===");
        System.out.printf("Results: %s%n", finalOutput);
        System.out.printf("Iterations: %d | Final delta: %.6f%n", iteration, lastDelta);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PageRankDriver(), args);
        System.exit(exitCode);
    }
}
