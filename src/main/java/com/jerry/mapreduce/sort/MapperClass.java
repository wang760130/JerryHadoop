package com.jerry.mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	private static IntWritable data = new IntWritable();
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		data.set(Integer.valueOf(line));
		context.write(data, new IntWritable(1));
	}
	
}
