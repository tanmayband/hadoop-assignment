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

public class SharesCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        private IntWritable num = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("|");
            word.set(record[1]);
            try {
                num.set(Integer.parseInt(record[2]));
            } catch (NumberFormatException e) {
                return;
            }
            System.out.println("<" + word.toString() + ", " + num.toString() + ">");
            context.write(word, num);
        }
    }

    public static class IntSharesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            System.out.println("<" + key.toString() + ", " + result.toString() + ">");
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "shares count");
        job.setJarByClass(SharesCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSharesReducer.class);
        job.setReducerClass(IntSharesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
