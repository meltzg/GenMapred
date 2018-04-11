package org.meltzg.genmapred.validator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GenJobValidatorReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
	private NullWritable nWritable = NullWritable.get();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count++;
		}
		
		if (count != 2) {
			context.write(key, nWritable);
		}
	}
}
