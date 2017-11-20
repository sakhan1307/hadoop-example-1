package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeatherMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	
	String fileName;

	protected void setup(
			Context context)
			throws java.io.IOException, InterruptedException {
		
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		fileName = fileSplit.getPath().getName();
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- 27516 20140101  2.424 -156.61   71.32   -16.6   -18.7   
		//-17.7   -17.7     0.0     0.00 C   -17.8   -19.4   -18.7   
		//83.8    73.5    80.8 -99.000 -99.000 -99.000 -99.000 -99.000 
		//-9999.0 -9999.0 -9999.0 -9999.0 -9999.0
		
		
		//Date, year, max_temp
		
		String date;
		int year;
		float max_temp;
		
		//Get the date
		
		final String iLine = value.toString();
		
		if(StringUtils.isNotEmpty(iLine))
		{
			date = StringUtils.substring(iLine, 6, 14).trim();//20140101
			year = Integer.parseInt(StringUtils.substring(date, 0,4).trim());
			
			max_temp= Float.parseFloat(StringUtils.substring(iLine, 38,45).trim());
			
			context.write(new IntWritable(year),  new Text(date+"\t"+max_temp+"\t"+fileName));
			
			
		}

		
	};

	protected void cleanup(
			Context context)
			throws java.io.IOException, InterruptedException {
	};

}
