package org.meltzg.genmapred.validator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenJobValidatorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public static String getOutputKeyClassName() {
		return Text.class.getCanonicalName();
	}
	
	public static String getOutputValueClassName() {
		return IntWritable.class.getCanonicalName();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(value, new IntWritable(1));
	}
}
