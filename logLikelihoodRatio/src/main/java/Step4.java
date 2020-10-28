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


//joining w1w2 count with w1 count
//input first map: step2 <w1 w2,decade,c12> 
//input second map: step1 <w1,decade,c1>
//output example: "w1 w2 \t decade \t c12 c1" <w1 w2,decade,c12 c1> 
public class Step4 {
	
    public static class DoublesMapper extends Mapper<LongWritable, Text, GramByDecade, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line = itr.nextToken().split("\t");
                String[] words = line[0].split("\\s+");
                if (words.length != 2) 
                    continue;          
                context.write(new GramByDecade(line[0],Integer.parseInt(line[1])), new Text(line[2]));              
            }
        }
    }

    public static class SinglesMapper extends Mapper<LongWritable, Text, GramByDecade, Text> {
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line = itr.nextToken().split("\t");
                String[] word = line[0].split("\\s+");
                if (word.length != 1) 
                    continue; 
                context.write(new GramByDecade(line[0],Integer.parseInt(line[1])), new Text(line[2]));
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer<GramByDecade, Text, GramByDecade, Text> {
    	
    	String c1;
    	@Override
        public void setup(Context context)  throws IOException, InterruptedException {
    		c1 = null;
        }
    	
        @Override
        protected void reduce(GramByDecade key, Iterable<Text> values, Context context) throws IOException, InterruptedException {     
            for(Text it : values) {
            	String[] line = it.toString().split("\t");
            	if(key.getText().toString().split(" ").length == 1) 
            		c1 = line[0];
            	else 
            		context.write(key, new Text(it.toString() + " " + c1));             	
            }               
        }
    }
    
    public static class PartitionerClass extends Partitioner<GramByDecade, Text> {
        @Override
        public int getPartition(GramByDecade key, Text value, int numPartitions) {  
            return (key.getDecade().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setPartitionerClass(PartitionerClass.class);
        
        job.setOutputKeyClass(GramByDecade.class);
        job.setOutputValueClass(Text.class);  
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DoublesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SinglesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}