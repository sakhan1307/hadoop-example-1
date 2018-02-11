package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustomerMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.ReducerJoinReducer;

public class ReducerJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// total 10 steps
		// Step:1
		// Get the configuration
		Configuration conf = super.getConf();

		// Setting to the configuration will go here

		// step-2 : Create Job instance

		Job reducerJoinDriver = Job.getInstance(conf,
				ReducerJoinDriver.class.getName());

		// step:3 : Set the classpath on the mappe DATANODE

		reducerJoinDriver.setJarByClass(ReducerJoinDriver.class);
		// step:4 : Setting the input

		// step: 4a : Set the customer input
		final String hdfsCustInput = args[0];
		// Convert to PATH
		final Path hdfsCustInputPath = new Path(hdfsCustInput);
		// Set this to the configuration
		MultipleInputs.addInputPath(reducerJoinDriver, hdfsCustInputPath,
				TextInputFormat.class, CustomerMapper.class);

		// step: 4b : Set the transaction input
		
		final String hdfsTxnInput = args[1];
		// Convert to PATH
		final Path hdfsTxnInputPath = new Path(hdfsTxnInput);
		// Set this to the configuration
		MultipleInputs.addInputPath(reducerJoinDriver, hdfsTxnInputPath,
				TextInputFormat.class, TxnMapper.class);

		// conf.set("mapreduce.input.dirs", hdfsInputPath);

		// step:5 : SEtting the output
		final String hdfsOutput = args[2];
		// Convert to the PATH
		final Path hdfsOutpath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(reducerJoinDriver, hdfsOutpath);

		reducerJoinDriver.setOutputFormatClass(TextOutputFormat.class);

		// step: 7 : Setting the reducer
		reducerJoinDriver.setReducerClass(ReducerJoinReducer.class);
		// step: 8 : SEtting mapper output key and value class
		// wordCountDriver.setMapOutputKeyClass(Text.class);
		// wordCountDriver.setMapOutputValueClass(IntWritable.class);

		// step:9 : SEtting reducer output key and value class

		reducerJoinDriver.setOutputKeyClass(Text.class);
		reducerJoinDriver.setOutputValueClass(Text.class);

		// ste:10 : trigger method.
		reducerJoinDriver.waitForCompletion(Boolean.TRUE);

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("In the MAIN method");
		// Validate inputs
		if (args.length < 3) {
			System.out.println("Java Usage " + ReducerJoinDriver.class
					+ " /path/to/hdfs/custs/input "
					+ " /path/to/hdfs/txn/input "
					+ " /path/to/hdfs/destination/directory");
			return;
		}
		// Loading configuraitons
		Configuration conf = new Configuration(Boolean.TRUE);
		// setting the classpath We will setup at runtime
		// Identifying the command fs --Not Required
		// Identifying the java class ---Not Required

		try {
			int i = ToolRunner.run(conf, new ReducerJoinDriver(), args);
			if (i == 0) {
				System.out.println("Success");
			}
		} catch (Exception e) {
			System.out.println("Failure");
			e.printStackTrace();
		}
	}

}
