
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//finding N for each decade
//input: step1 <w1,decade,c1>  
//output example: "199 \t 30000" <decade,N>  
public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
  
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t");
                String[] words = line[0]. split("\\s+");
                if (words.length != 1) 
                    continue; 
                context.write(new Text(line[1]),new IntWritable(Integer.parseInt(line[2])));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class CombinerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {  
            return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(CombinerClass.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);				
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
