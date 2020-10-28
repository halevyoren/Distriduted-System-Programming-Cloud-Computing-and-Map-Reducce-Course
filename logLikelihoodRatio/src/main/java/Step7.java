
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//calculate log lambda
//input: step6 <w1 w2,decade,c12 c1 c2 N>
//output example: "w1 w2 \t 199 \t 0.1" <w1 w2,decade,likelihood ratio>   
public class Step7 {
    public static class MapperClass extends Mapper<LongWritable, Text, GramByDecade, Text> {
  
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t");
                String[] words = line[0]. split("\\s+");
                if (words.length != 2) 
                    continue; 
                context.write(new GramByDecade(line[0],Integer.parseInt(line[1])),new Text(line[2]));
            }
        }
    }

    public static class ReducerClass extends Reducer<GramByDecade,Text,GramByDecade,DoubleWritable> {
    	
        @Override
        public void reduce(GramByDecade key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        	for (Text value : values) {
        		String nullStr = "null";
                String[] parts = value.toString().split("\\s+");
                if(parts[0].equals(nullStr) || parts[1].equals(nullStr) || parts[2].equals(nullStr) || parts[3].equals(nullStr))
                	continue;
                int c12 = Integer.parseInt(parts[0]);
                int c1 = Integer.parseInt(parts[1]);
                int c2 = Integer.parseInt(parts[2]);
                int N = Integer.parseInt(parts[3]);
                double probability = calcRatio(c12,c1,c2,N);
                context.write(key, new DoubleWritable((-2)*probability));
            }
        }
        private double calcRatio(int c12,int c1,int c2,int N) {
        	double result;
        	double p,p1,p2;
        	p = (double)c2/N;
        	p1 = (double)c12/c1;
        	p2 = (double)(c2-c12)/(N-c1);
        	result = java.lang.Math.log(calcL(c12,c1,p))+java.lang.Math.log(calcL(c2-c12,N-c1,p))-java.lang.Math.log(calcL(c12,c1,p1))-java.lang.Math.log(calcL(c2-c12,N-c1,p2));
        	return result;
        }
        
        private double calcL(double k, double n, double x) {
        	return java.lang.Math.pow(x, k)*java.lang.Math.pow(1-x,n-k);
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
        Job job = Job.getInstance(conf, "Step7");
        job.setJarByClass(Step7.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setMapOutputKeyClass(GramByDecade.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(GramByDecade.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);				
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

