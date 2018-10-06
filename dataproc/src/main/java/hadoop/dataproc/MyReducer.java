package hadoop.dataproc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer {

	public static class TestReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			System.out.println("Inside Reducer Method.");
			for(Text value : values){
				
				context.write(key, value);
				
			}
		}
		
	}
	
}
