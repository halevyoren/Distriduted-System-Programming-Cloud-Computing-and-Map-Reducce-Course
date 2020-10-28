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

//joining w1w2_w1 count with w2 count
//input first map: step4 <w1 w2,decade,c12 c1> 
//input second map: step1 <w2,decade,c2>
//output example: "w1 w2 \t decade \t c12 c1 c2" <w1 w2,decade,c12 c1 c2> 
public class Step5 {
    public static class DoublesMapper extends Mapper<LongWritable, Text, GramByDecade, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line = itr.nextToken().split("\t");
                String[] words = line[0].split("\\s+");
                if (words.length != 2) 
                    continue;          
                context.write(new GramByDecade(words[1] + " " + words[0],Integer.parseInt(line[1])), new Text(line[2]));
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
    	
    	String c2;
    	@Override
        public void setup(Context context)  throws IOException, InterruptedException {
    		c2 = null;
        }
    	
        @Override
        protected void reduce(GramByDecade key, Iterable<Text> values, Context context) throws IOException, InterruptedException {     
            for(Text it : values) {
            	String[] line = it.toString().split("\t");
            	if(key.getText().toString().split("\\s+").length == 1) 
            		c2 = line[0];
            	else {
            		String[] newKey = key.getText().toString().split(" ");
            		context.write(new GramByDecade(newKey[1] + " " + newKey[0],key.getDecade().get()), new Text(it.toString() + " " + c2));    
            	}
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
        Job job = Job.getInstance(conf, "Step5");
        job.setJarByClass(Step5.class);
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