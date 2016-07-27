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

public class UniqueListeners {

    private static final IntWritable one = new IntWritable(1);

    public static class IdExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {
       
        private Text userId = new Text();
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.split("|");
            word.set(record[0]);
            context.write(word, one);
        }
    }

    public static class IntCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(
