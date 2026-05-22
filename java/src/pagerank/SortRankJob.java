package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * SortRankJob — sort final PageRank results by rank (descending).
 *
 * Input:  "node\trank|neighbors"
 * Output: "node\trank" (sorted by rank, highest first)
 *
 * Technique: emit (rank, node) as key so Hadoop sorts by rank,
 * then use a custom descending comparator.
 */
public class SortRankJob {

    // -----------------------------------------------------------------------
    // Mapper: emit (rank, node) for sorting
    // -----------------------------------------------------------------------
    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) return;

            String[] parts = line.split("\t", 2);
            if (parts.length != 2) return;

            String node  = parts[0].trim();
            String state = parts[1].trim();

            int pipe = state.indexOf('|');
            double rank = (pipe >= 0) ? Double.parseDouble(state.substring(0, pipe))
                                      : Double.parseDouble(state);

            context.write(new DoubleWritable(rank), new Text(node));
        }
    }

    // -----------------------------------------------------------------------
    // Reducer: emit (node, rank)
    // -----------------------------------------------------------------------
    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {

        @Override
        protected void reduce(DoubleWritable rank, Iterable<Text> nodes, Context context)
                throws IOException, InterruptedException {

            for (Text node : nodes) {
                context.write(node, new Text(String.format("%.8f", rank.get())));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Descending comparator for DoubleWritable keys
    // -----------------------------------------------------------------------
    public static class DescendingDoubleComparator extends WritableComparator {

        public DescendingDoubleComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // Reverse natural order → highest rank first
            return -super.compare(a, b);
        }
    }

    // -----------------------------------------------------------------------
    // Job factory
    // -----------------------------------------------------------------------
    public static Job createJob(Configuration conf, String inputPath, String outputPath)
            throws Exception {

        Job job = Job.getInstance(conf, "PageRank-Sort");
        job.setJarByClass(SortRankJob.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setSortComparatorClass(DescendingDoubleComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Single reducer to produce one sorted output file
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
