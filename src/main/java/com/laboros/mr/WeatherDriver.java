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

import com.laboros.mapper.WeatherMapper;
import com.laboros.reducer.WeatherReducer;

//load weather datasets into hdfs
//$ hadoop fs -put Datasets/Weather/ /user/edureka/

//$ yarn jar hadoop-example-1-0.0.1-SNAPSHOT.jar com.laboros.mr.WeatherDriver /user/edureka/Weather /user/edureka/WEATHER_OP

public class WeatherDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//total 10 steps
		//Step:1
		//Get the configuration
		Configuration conf = super.getConf();
		
		//Setting to the configuration will go here
		
		//step-2 : Create Job instance
		
		Job weatherJob = Job.getInstance(conf, WeatherDriver.class.getName());
		
		//step:3 : Set the classpath on the mappe DATANODE
		
		
		weatherJob.setJarByClass(WeatherDriver.class);
		//step:4 : Setting the input
		
		final String hdfsInput = args[0];
		//Convert to PATH
		final Path hdfsInputPath = new Path(hdfsInput);
		//Set this to the configuration
		TextInputFormat.addInputPath(weatherJob, hdfsInputPath);
		weatherJob.setInputFormatClass(TextInputFormat.class);
//		conf.set("mapreduce.input.dirs", hdfsInputPath);
		
		
		//step:5 : SEtting the output
		final String hdfsOutput = args[1];
		//Convert to the PATH
		final Path hdfsOutpath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(weatherJob, hdfsOutpath);
		
		weatherJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step:6 : Setting the mapper
		
		weatherJob.setMapperClass(WeatherMapper.class);
		//step: 7 : Setting the reducer
		weatherJob.setReducerClass(WeatherReducer.class);
		//step: 8 : SEtting mapper output key and value class
//		weatherJob.setMapOutputKeyClass(IntWritable.class);
//		weatherJob.setMapOutputValueClass(Text.class);
		
		//step:9 : SEtting reducer output key and value class
		
		weatherJob.setOutputKeyClass(IntWritable.class);
		weatherJob.setOutputValueClass(Text.class);
		
		//step:10 : trigger method.
		weatherJob.waitForCompletion(Boolean.TRUE);
		
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
							+ WeatherDriver.class
							+ " /path/to/hdfs/file /path/to/hdfs/destination/directory");
			return;
		}
		// Loading configuraitons
		Configuration conf = new Configuration(Boolean.TRUE);
		// setting the classpath We will setup at runtime
		// Identifying the command fs --Not Required
		// Identifying the java class ---Not Required

		try {
			int i = ToolRunner.run(conf, new WeatherDriver(), args);
			if (i == 0) {
				System.out.println("Success");
			}
		} catch (Exception e) {
			System.out.println("Failure");
			e.printStackTrace();
		}
	}

}
