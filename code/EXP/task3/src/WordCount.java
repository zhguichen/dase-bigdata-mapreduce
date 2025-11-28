import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Get slowstart parameter from command line arguments
        String slowstart = "0.50"; // default
        if (args.length >= 3) {
            slowstart = args[2];
            conf.set("mapreduce.job.reduce.slowstart.completedmaps", slowstart);
        }
        
        // Get number of reducers from command line arguments
        int numReducers = 4; // default
        if (args.length >= 4) {
            numReducers = Integer.parseInt(args[3]);
        }
        
        // Extract workload type from input path if possible
        // Input path format: /user/root/task3/input_wordcount or input_terasort
        String workloadType = "WordCount";
        if (args.length >= 1) {
            String inputPath = args[0];
            if (inputPath.toLowerCase().contains("terasort")) {
                workloadType = "TeraSort";
            } else if (inputPath.toLowerCase().contains("wordcount")) {
                workloadType = "WordCount";
            }
        }
        
        // Build job name with experiment information
        // Task3: Workload type comparison (IO-intensive vs CPU-intensive)
        String jobName = String.format("Task3_%s_slowstart%s_reducers%d", 
                                       workloadType, slowstart, numReducers);
        
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Set number of reduce tasks
        job.setNumReduceTasks(numReducers);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

