package com.jerry.hadoop.mapreducer.access.accessip;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jerry.hadoop.mapreducer.access.Access;

public class MapperClass extends Mapper<Object, Text, Text, IntWritable>{
	
	private Text word = new Text();
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		Access access = Access.parser(line);
		if(access != null) {
			word.set(access.getRemoteIp());
			context.write(word, one); 
		} else {
			System.out.println(line);
		}
	}
}