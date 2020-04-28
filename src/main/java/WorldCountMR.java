import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
        int sum = 0;
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value: values){
                sum += value.get();
            }
            v.set(sum);
            context.write(key,v);
        }
    }
}
