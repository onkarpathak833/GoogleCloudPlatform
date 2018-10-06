package hadoop.dataproc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper {
	
	public static class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) {
			
			String allData[] = value.toString().split(",");
			String summonsNumber = allData[0];
			String dateValue = allData[4];
			String monthValue = "";
			String yearValue = "";
			if(dateValue.split("/").length==3) {
				 monthValue = dateValue.split("/")[0].trim();
				 yearValue = dateValue.split("/")[2].trim();
			}
			
			System.out.println("Inside Mapper Function.");
			if((monthValue.equals("08") && yearValue.equals("2014")) || (monthValue.equals("08") && yearValue.equals("2015"))) {
				System.out.println("Condition is true.");
				try {
					System.out.println("Summon Number : "+summonsNumber);
					context.write(new Text(summonsNumber), value);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		}
		
	}

}
