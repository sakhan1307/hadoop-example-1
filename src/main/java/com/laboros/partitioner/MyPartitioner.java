package com.laboros.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		final String inputKey = key.toString();
		if(StringUtils.isNotEmpty(inputKey))
		{
			if(StringUtils.equalsIgnoreCase(inputKey, "hadoop"))
			{
				return 0;
			}
			if(StringUtils.equalsIgnoreCase(inputKey, "data"))
			{
				return 1;
			}
		}
		return 2;
	}

}
