package com.jerry.mapreduce.stjoin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String childname = new String();
		String perentname = new String();
		// 左右表标识
		String relationtype = new String();
		StringTokenizer itr = new StringTokenizer(value.toString());
		String[] values = new String[2];
		int i = 0;
		
		while(itr.hasMoreElements()) {
			values[i] = itr.nextToken();
			i++;
		}
		
		if (values[0].compareTo("child") != 0) {
			childname = values[0];
			perentname = values[1];
			
			// 输出左表
			relationtype = "1";
			context.write(new Text(perentname), new Text(relationtype + "+" + childname + "+" + perentname));
			
			// 输出右表
			relationtype = "2";
			context.write(new Text(childname), new Text(relationtype + "+" + childname + "+" + perentname));
		}
	} 
}
