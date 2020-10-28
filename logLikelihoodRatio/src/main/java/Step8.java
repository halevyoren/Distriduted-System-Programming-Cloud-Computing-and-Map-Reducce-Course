

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//take top-100 collocations for each decade
//input: step7 <w1 w2,decade,likelihood ratio>
//output example: "w1 w2 \t 199 \t 0.1" <w1 w2,decade,likelihood ratio>   
public class Step8 {
	
    public static class MapperClass extends Mapper<LongWritable, Text, TopColl, Text> {
    	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t");
                String[] words = line[0].split("\\s+");
                if (words.length != 2) 
                    continue; 
                context.write(new TopColl(line[1],line[2]),new Text(line[0]));
            }
        }
    }

    public static class ReducerClass extends Reducer<TopColl,Text,GramByDecade,DoubleWritable> {
    	String decade = null;
    	int count = 0;
        @Override
        public void reduce(TopColl key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        	String[] keyParts = key.toString().split("\\s+");
        	if(decade == null || !decade.equals(keyParts[0])) {
        		decade = keyParts[0];
        		count = 0;
        	}
        	for (Text value : values) {        		
        		if(count >= 100)
        			continue;
        		count++;
        		context.write(new GramByDecade(value.toString(),Integer.parseInt(keyParts[0])),new DoubleWritable(Double.parseDouble(keyParts[1])));
            }
        }
        
    }

    public static class PartitionerClass extends Partitioner<TopColl, Text> {
        @Override
        public int getPartition(TopColl key, Text value, int numPartitions) {  
            return (String.valueOf(key.decade.get()).toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step8");
        job.setJarByClass(Step8.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setMapOutputKeyClass(TopColl.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(GramByDecade.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);				
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


class TopColl implements WritableComparable<TopColl> {
	IntWritable decade;
	DoubleWritable ratio;
	
    
	TopColl(){
		this.decade = new IntWritable();
		this.ratio = new DoubleWritable();
	}
	
	TopColl(String decade, String ratio) {
		this.decade = new IntWritable(Integer.parseInt(decade));
        this.ratio = new DoubleWritable(Double.parseDouble(ratio));   
    }     
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	decade.readFields(in); 
        ratio.readFields(in);  
    }
    @Override
    public void write(DataOutput out) throws IOException {
    	decade.write(out);
    	ratio.write(out);
    }
    
    @Override
    public int compareTo(TopColl other) {
    	int result = decade.get() - other.decade.get();
    	if(result == 0) 
    		result = other.ratio.compareTo(ratio); 	
        return result;       	
    } 
    
    public String toString() { 
  	  return decade.get() + " " +ratio.get();  
  } 
}


