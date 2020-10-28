import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class GramByDecade implements WritableComparable<GramByDecade> {
    private Text gram2;
    private IntWritable decade;
    
    GramByDecade() {
        gram2 = new Text();
        decade = new IntWritable(); 
    }
    
    GramByDecade(String text, String year) {
        gram2 = new Text(text);
        decade = new IntWritable(Integer.parseInt(year.substring(0,year.length()-1)));
    }
    
    GramByDecade(String text, int year) {
        gram2 = new Text(text);
        decade = new IntWritable(year);
    }
     
    public Text getText() {
    	return gram2;
    }
    
    public IntWritable getDecade() {
    	return decade;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	gram2.readFields(in); 
        decade.readFields(in);  
    }
    @Override
    public void write(DataOutput out) throws IOException {
    	gram2.write(out);
    	decade.write(out);
    }
    
    @Override
    public int compareTo(GramByDecade other) {
    	int result = getDecade().get() - other.getDecade().get();
    	if(result == 0)
    		result = getText().toString().compareTo(other.getText().toString());
        return result;       	
    }
    
    public String toString() { 
    	  return gram2.toString() + "\t" +String.valueOf(decade.get());  
    }  
    
}