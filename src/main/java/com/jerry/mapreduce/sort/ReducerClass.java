package com.jerry.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

	private static IntWritable linenum = new IntWritable(1);
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		context.write(linenum, key);
		linenum = new IntWritable(linenum.get() + 1);
	}
	
}
