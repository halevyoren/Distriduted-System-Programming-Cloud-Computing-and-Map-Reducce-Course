
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//finding all word by decade in 2 gram.
//input: 2gram
//output example: "w1 w2 \t 199 \t 15" <w1 w2,decade,c12>  
public class Step2 {
    
    public static class MapperClass extends Mapper<LongWritable, Text, GramByDecade, IntWritable> {
    	StopWords constWords;
    	@Override
    	public void setup(Context context)  throws IOException, InterruptedException {
    		String lang = context.getConfiguration().get("lang","1");
            constWords = new StopWords(lang);
    	}
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t");
                String[] words = line[0]. split("\\s+");
                if (words.length != 2 || constWords.isStopWord(words[0]) || constWords.isStopWord(words[1])) 
                    continue; 
                context.write(new GramByDecade(line[0],line[1]),new IntWritable(Integer.parseInt(line[2])));
            }
        }
    }

    public static class ReducerClass extends Reducer<GramByDecade,IntWritable,GramByDecade,IntWritable> {
        @Override
        public void reduce(GramByDecade key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class CombinerClass extends Reducer<GramByDecade,IntWritable,GramByDecade,IntWritable> {
        @Override
        public void reduce(GramByDecade key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<GramByDecade, IntWritable> {
        @Override
        public int getPartition(GramByDecade key, IntWritable value, int numPartitions) {  
            return (key.getDecade().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("lang", args[2]);
        
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(CombinerClass.class);
        
        job.setMapOutputKeyClass(GramByDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(GramByDecade.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(SequenceFileInputFormat.class);				//SequenceFileInputFormat.class
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
