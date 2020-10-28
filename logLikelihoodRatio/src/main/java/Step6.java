import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//joining w1w2_w1_w2 count with N count
//input first map: step5 <w1 w2,decade,c12 c1 c2> 
//input second map: step3 <decade,N>  
//output example: "w1 w2 \t decade \t c12 c1 c2 N" <w1 w2,decade,c12 c1 c2 N> 
public class Step6 {
    public static class DoublesMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line = itr.nextToken().split("\t");
                String[] words = line[0].split("\\s+");
                if (words.length != 2) 
                    continue;          
                context.write(new Text(line[1] + " " + "a"), new Text(line[0] + "\t" + line[2]));
            }
        }
    }

    public static class SinglesMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line = itr.nextToken().split("\t");
                String[] word = line[0].split("\\s+");
                if (word.length != 1) 
                    continue; 
                context.write(new Text(line[0]), new Text(line[1]));
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, GramByDecade, Text> {
    	
    	String N;
    	@Override
        public void setup(Context context)  throws IOException, InterruptedException {
    		N = null;
        }
    	
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {        
            for(Text it : values) {
            	String[] line = it.toString().split("\t");
            	String[] decade = key.toString().split("\\s+");
            	if(decade.length == 1) {
            		N = line[0];
            	}else {
            		context.write(new GramByDecade(line[0],Integer.parseInt(decade[0])), new Text(line[1] + " " + N)); 
            	}
            }
        }
    }
    
    public static class PartitionerClass extends Partitioner<Text, Text> {
	    @Override
	    public int getPartition(Text key, Text value, int numPartitions) {  
	        return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
	    }
	}
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step6");
        job.setJarByClass(Step6.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setPartitionerClass(PartitionerClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DoublesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SinglesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}