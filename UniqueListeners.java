/**
 * Author: Abhijeet Krishnan
 * Enroll. No.: BT13CSE001
 * Software Lab III - Assignment 1 - Question 1
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;

public class UniqueListeners {
    private static final IntWritable one = new IntWritable(1);
    
    public static class IdExtractorMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text userId = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\\|");
            userId.set(record[0]);
            context.write(one, userId);
        }
    }
    
    public static class IntCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> userIds = new HashSet<String>();
            for (Text t : values) {
                userIds.add(t.toString()); // horrible way to do this
            }
            Text message = new Text("Total count");
            IntWritable num = new IntWritable(userIds.size() - 1);
            context.write(message, num);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unique listeners");
        job.setJarByClass(UniqueListeners.class);
        job.setMapperClass(IdExtractorMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IntCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
