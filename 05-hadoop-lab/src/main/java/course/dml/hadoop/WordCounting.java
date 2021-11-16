package course.dml.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

@Slf4j
public class WordCounting {

    public static class TokenizerMapper
            extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        static enum MyCounters {NUM_RECORDS}

        private String mapTaskId;
        private String inputFile;
        private int noRecords = 0;

        public void configure(JobConf job) {
            mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
            inputFile = job.get(JobContext.MAP_INPUT_FILE);
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            log.info("!!!!! Object Key {}", key.toString());

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, one);
                // Increment the no. of <key, value> pairs processed
                ++noRecords;

                // Increment counters
                reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

                // Every 100 records update application-level status
//                if ((noRecords%100) == 0) {
                reporter.setStatus(mapTaskId + " processed " + noRecords +
                        " from input-file: " + inputFile);
//                }

            }
        }
    }

    public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            for (Iterator<IntWritable> it = values; it.hasNext(); ) {
                IntWritable val = it.next();
                sum += val.get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // Create a new JobConf
        JobConf job = new JobConf(conf, WordCounting.class);
        job.setJobName("WordCounting");
        job.setJarByClass(WordCounting.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outPath = new Path(args[0]);
        outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);
        for (int i = 1; i < args.length; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        JobClient.runJob(job);
    }
}
