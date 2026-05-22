package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GraphInitJob
 *
 * Reads raw edge list and emits initial PageRank state:
 *   Input:  "src\tdst"
 *   Output: "node\t1.0|neighbor1,neighbor2,..."
 *
 * Note: The initial rank is set to 1.0 (un-normalised) because N is not known
 *       within a single init job. This is numerically safe: the PageRank update
 *       is self-normalising — the total mass converges to 1 regardless of the
 *       starting sum (fixed point of S -> (1-d) + d*S is S = 1) — so the final
 *       ranks are correct. It only costs a few extra early iterations. The exact
 *       node count N is exposed via the NUM_NODES counter for the iteration job.
 */
public class GraphInitJob {

    public enum Counters {
        NUM_NODES
    }

    // -----------------------------------------------------------------------
    // Mapper: emit (src, "LINK:dst") and (dst, "NODE") for presence tracking
    // -----------------------------------------------------------------------
    public static class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text LINK_PREFIX = new Text();
        private static final Text NODE_MARKER = new Text("NODE");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) return;

            String[] parts = line.split("\t");
            if (parts.length != 2) return;

            String src = parts[0].trim();
            String dst = parts[1].trim();

            // Emit the edge: src -> dst
            context.write(new Text(src), new Text("LINK:" + dst));

            // Ensure dst node exists (may be dangling — no outlinks)
            context.write(new Text(dst), NODE_MARKER);
        }
    }

    // -----------------------------------------------------------------------
    // Reducer: assemble adjacency list and emit initial state
    // -----------------------------------------------------------------------
    public static class InitReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text node, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> neighbors = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("LINK:")) {
                    neighbors.add(v.substring(5));
                }
                // "NODE" values just ensure the node exists; no action needed
            }

            // Count this node
            context.getCounter(Counters.NUM_NODES).increment(1);

            // Initial rank = 1.0 (will be normalized in iteration 1)
            String neighborsStr = String.join(",", neighbors);
            context.write(node, new Text("1.0|" + neighborsStr));
        }
    }

    // -----------------------------------------------------------------------
    // Job factory
    // -----------------------------------------------------------------------
    public static Job createJob(Configuration conf, String inputPath, String outputPath)
            throws Exception {

        Job job = Job.getInstance(conf, "PageRank-GraphInit");
        job.setJarByClass(GraphInitJob.class);

        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
