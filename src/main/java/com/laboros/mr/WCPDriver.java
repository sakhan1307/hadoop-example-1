package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.partitioner.MyPartitioner;
import com.laboros.reducer.WordCountReducer;

public class WCPDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//total 10 steps
		//Step:1
		//Get the configuration
		Configuration conf = super.getConf();
		
		//Setting to the configuration will go here
		
		//step-2 : Create Job instance
		
		Job wordCountDriver = Job.getInstance(conf, WCPDriver.class.getName());
		
		//step:3 : Set the classpath on the mappe DATANODE
		
		
		wordCountDriver.setJarByClass(WCPDriver.class);
		//step:4 : Setting the input
		
		final String hdfsInput = args[0];
		//Convert to PATH
		final Path hdfsInputPath = new Path(hdfsInput);
		//Set this to the configuration
		TextInputFormat.addInputPath(wordCountDriver, hdfsInputPath);
		wordCountDriver.setInputFormatClass(TextInputFormat.class);
//		conf.set("mapreduce.input.dirs", hdfsInputPath);
		
		
		//step:5 : SEtting the output
		final String hdfsOutput = args[1];
		//Convert to the PATH
		final Path hdfsOutpath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(wordCountDriver, hdfsOutpath);
		
		wordCountDriver.setOutputFormatClass(TextOutputFormat.class);
		
		//step:6 : Setting the mapper
		
		wordCountDriver.setMapperClass(WordCountMapper.class);
		//step: 7 : Setting the reducer
		wordCountDriver.setReducerClass(WordCountReducer.class);
		wordCountDriver.setCombinerClass(WordCountReducer.class);
		wordCountDriver.setPartitionerClass(MyPartitioner.class);
		wordCountDriver.setNumReduceTasks(3);
		//step: 8 : SEtting mapper output key and value class
//		wordCountDriver.setMapOutputKeyClass(Text.class);
//		wordCountDriver.setMapOutputValueClass(IntWritable.class);
		
		//step:9 : SEtting reducer output key and value class
		
		wordCountDriver.setOutputKeyClass(Text.class);
		wordCountDriver.setOutputValueClass(IntWritable.class);
		
		//ste:10 : trigger method.
		wordCountDriver.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("In the MAIN method");
		// Validate inputs
		if (args.length < 2) {
			System.out
					.println("Java Usage "
							+ WCPDriver.class
							+ " /path/to/hdfs/file /path/to/hdfs/destination/directory");
			return;
		}
		// Loading configuraitons
		Configuration conf = new Configuration(Boolean.TRUE);
		// setting the classpath We will setup at runtime
		// Identifying the command fs --Not Required
		// Identifying the java class ---Not Required

		try {
			int i = ToolRunner.run(conf, new WCPDriver(), args);
			if (i == 0) {
				System.out.println("Success");
			}
		} catch (Exception e) {
			System.out.println("Failure");
			e.printStackTrace();
		}
	}

}
