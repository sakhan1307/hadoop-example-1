package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
	};

	@Override
	protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		// key -- 2014
		// value {20140101 12.5,20140102 65.5 20141211 55.3}

		// year date max

		String max_temp_recorded_date = null;
		float identified_max_temp = Float.MIN_VALUE;
		String fileName = null;

		for (Text date_max_temp : values) {

			final String[] tokens = StringUtils.splitPreserveAllTokens(
					date_max_temp.toString(), "\t");
			
			//tokens[0] = date
			//tokens[1] = max_temp
			
			if(identified_max_temp < Float.parseFloat(tokens[1]))
			{
			
				identified_max_temp=Float.parseFloat(tokens[1]);
				max_temp_recorded_date = tokens[0];
				fileName = tokens[2];
			}
		}
		
		context.write(key, new Text(max_temp_recorded_date + "\t" + identified_max_temp+"\t"+fileName));
	};

	protected void cleanup(Context context) throws java.io.IOException,
			InterruptedException {
	};
}
