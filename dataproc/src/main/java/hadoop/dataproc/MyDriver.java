package hadoop.dataproc;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import hadoop.dataproc.MyMapper.TestMapper;
import hadoop.dataproc.MyReducer.TestReducer;


public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Test");
		job.setJarByClass(MyDriver.class);
		
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//System.out.println("System Arguments : "+args[0]+ "  & "+args[1]);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		System.out.println(args[0] + " - "+args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Random rand = new Random();
		String suffix = String.valueOf(rand.nextInt(100));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+suffix));
		
		job.waitForCompletion(true);
	}

}
