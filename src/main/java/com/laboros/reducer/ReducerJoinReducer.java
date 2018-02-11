package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		// key --4000001
		// values --{TXNS 092.88,TXNS 180.35,TXNS 051.18,CUSTS Kristina}

		String name = null;
		long count = 0;
		double sum = 0;

		for (Text value : values) {
			// value --- TXNS 092.88 OR CUSTS Kristina

			final String tokens[] = StringUtils.splitPreserveAllTokens(
					value.toString(), "\t");
			if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS"))
			{
				count++;
				sum = Double.parseDouble(tokens[1])+sum;
			}else {
				name = tokens[1];
			}
		}
		if(StringUtils.isNotEmpty(name))
		{
		context.write(new Text(name), new Text(count + "\t" + sum));
		}
	};
}
