import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorldCountMR {

    public static class WorldCountMRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            Text k = new Text();
            IntWritable v = new IntWritable(1);
            for (String word : words){
                k.set(word);
                context.write(k,v);
            }
        }
    }


    public static class WorldCountMRReduce extends Reducer<Text, IntWritable,Text, IntWritable>{

        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable v = new IntWritable();
            for (IntWritable value: values){
                sum += value.get();
            }
            v.set(sum);
            context.write(key,v);
        }
    }

    public static class WorldCountMRDriver{
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);

            job.setJarByClass(WorldCountMRDriver.class);

            job.setMapperClass(WorldCountMRMapper.class);
            job.setReducerClass(WorldCountMRReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job,new Path("D:\\编程文件夹\\hadoop-learn\\src\\main\\resources\\i have a dream.txt"));
            FileOutputFormat.setOutputPath(job,new Path("D:\\编程文件夹\\hadoop-learn\\src\\main\\resources\\count.txt"));

            boolean res = job.waitForCompletion(true);
            System.exit(res ? 0 : 1);
        }
    }
}
