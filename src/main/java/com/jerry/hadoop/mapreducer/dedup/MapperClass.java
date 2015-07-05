package com.jerry.hadoop.mapreducer.dedup;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, Text>{
	private static Text line=new Text();//每行数据
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		line = value;
		context.write(line, new Text(""));
	}
}
