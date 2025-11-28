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
        
        // Extract task type and data size from input path
        // Input path format: /user/root/task2/input_wordcount_500MB or input_terasort_1GB
        String taskType = "WordCount";
        String dataSize = "unknown";
        if (args.length >= 1) {
            String inputPath = args[0];
            // Extract task type and data size from path
            int lastSlash = inputPath.lastIndexOf('/');
            String pathPart = lastSlash >= 0 ? inputPath.substring(lastSlash + 1) : inputPath;
            if (pathPart.startsWith("input_")) {
                String afterInput = pathPart.substring(6); // Remove "input_" prefix
                // Check for task type prefix (wordcount_ or terasort_)
                if (afterInput.startsWith("wordcount_")) {
                    taskType = "WordCount";
                    dataSize = afterInput.substring(10); // Remove "wordcount_" prefix
                } else if (afterInput.startsWith("terasort_")) {
                    taskType = "TeraSort";
                    dataSize = afterInput.substring(9); // Remove "terasort_" prefix
                } else {
                    // Legacy format: just data size
                    taskType = "WordCount";
                    dataSize = afterInput;
                }
            }
        }
        
        // Build job name with experiment information
        // Task2: Data scalability testing with different data sizes
        // Priority: Use job name from configuration if set (via -Dmapreduce.job.name),
        // otherwise build from parameters
        String jobName = conf.get("mapreduce.job.name");
        if (jobName == null || jobName.isEmpty()) {
            // Fallback: build job name from parameters
            jobName = String.format("Task2_%s_%s_slowstart%s_reducers%d", 
                                   taskType, dataSize, slowstart, numReducers);
        }
        
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

