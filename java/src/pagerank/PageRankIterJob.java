package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * PageRankIterJob — one iteration of PageRank.
 *
 * Input format (from previous iteration or GraphInitJob):
 *   node_id   rank|neighbor1,neighbor2,...
 *
 * Output format (same structure, updated rank):
 *   node_id   new_rank|neighbor1,neighbor2,...
 *
 * Mapper emits:
 *   (neighbor, "RANK:share")      — rank contribution
 *   (node,     "GRAPH:neighbors") — preserve structure
 *
 * Reducer:
 *   Applies PR formula: new_rank = (1 - d)/N + d * sum(contributions)
 *   Tracks |delta| via Hadoop counter (precision: 1e-8)
 */
public class PageRankIterJob {

    public enum Counters {
        DELTA_SUM    // stored as (delta * 1e8) to avoid float counters
    }

    // -----------------------------------------------------------------------
    // Mapper
    // -----------------------------------------------------------------------
    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable offset, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) return;

            String[] parts = line.split("\t", 2);
            if (parts.length != 2) return;

            String node  = parts[0].trim();
            String state = parts[1].trim();

            // Parse state: "rank|neighbors"
            int pipe = state.indexOf('|');
            if (pipe < 0) return;

            double rank        = Double.parseDouble(state.substring(0, pipe));
            String neighborsStr = state.substring(pipe + 1);
            String[] neighbors = neighborsStr.isEmpty() ? new String[0]
                                                        : neighborsStr.split(",");

            // 1. Preserve graph structure for reducer
            context.write(new Text(node), new Text("GRAPH:" + neighborsStr));

            // 1b. Preserve the node's own (old) rank so the reducer can compute
            //     the exact L1 delta |new_rank - old_rank| for convergence.
            context.write(new Text(node), new Text("OLDRANK:" + rank));

            // 2. Distribute rank to each out-neighbor
            if (neighbors.length > 0) {
                double share = rank / neighbors.length;
                String shareStr = "RANK:" + String.format("%.10f", share);
                for (String nb : neighbors) {
                    context.write(new Text(nb), new Text(shareStr));
                }
            }
            // Dangling nodes (no outlinks): their rank is lost in this version.
            // For production accuracy, collect dangling mass and redistribute.
        }
    }

    // -----------------------------------------------------------------------
    // Reducer
    // -----------------------------------------------------------------------
    public static class PRReducer extends Reducer<Text, Text, Text, Text> {

        private double damping;
        private long   numNodes;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            damping  = conf.getDouble(PageRankDriver.CONF_DAMPING, 0.85);
            numNodes = conf.getLong(PageRankDriver.CONF_NUM_NODES, 1L);
        }

        @Override
        protected void reduce(Text node, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String neighborsStr = "";
            double oldRank      = 0.0;   // approximate from last state (stored)
            double rankSum      = 0.0;
            boolean hasOldRank  = false;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("GRAPH:")) {
                    neighborsStr = v.substring(6);
                } else if (v.startsWith("RANK:")) {
                    rankSum += Double.parseDouble(v.substring(5));
                } else if (v.startsWith("OLDRANK:")) {
                    oldRank = Double.parseDouble(v.substring(8));
                    hasOldRank = true;
                }
            }

            // PageRank formula
            double newRank = (1.0 - damping) / numNodes + damping * rankSum;

            // Exact convergence delta |new_rank - old_rank|, accumulated as an
            // integer counter (scaled by 1e8 because Hadoop counters are longs).
            // The mapper now forwards each node's previous rank via "OLDRANK:".
            // Fallback to the uniform 1/N baseline only if it is missing (e.g.
            // a node that received no GRAPH/OLDRANK record).
            double previousRank = hasOldRank ? oldRank : (1.0 / numNodes);
            long deltaInt = Math.round(Math.abs(newRank - previousRank) * 1e8);
            context.getCounter(Counters.DELTA_SUM).increment(deltaInt);

            // Emit updated state
            context.write(node, new Text(
                String.format("%.8f|%s", newRank, neighborsStr)
            ));
        }
    }

    // -----------------------------------------------------------------------
    // Job factory
    // -----------------------------------------------------------------------
    public static Job createJob(Configuration conf, String inputPath, String outputPath)
            throws Exception {

        Job job = Job.getInstance(conf, "PageRank-Iteration");
        job.setJarByClass(PageRankIterJob.class);

        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Use multiple reducers for large graphs
        job.setNumReduceTasks(conf.getInt("pagerank.reducers", 1));

        return job;
    }
}
