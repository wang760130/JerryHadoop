package com.jerry.mapreduce.score;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		// 将输入的数据首先按行进行分割
		StringTokenizer line  = new StringTokenizer(value.toString());
		
		// 分别对每一行进行处理
		while(line.hasMoreElements()) {
			
			// 学生姓名部分
			String nameStr = line.nextToken();

			// 成绩部分
            String scoreStr = line.nextToken();
            
            Text name = new Text(nameStr);
            int score = Integer.parseInt(scoreStr);
            
            // 输出姓名和成绩
            context.write(name, new IntWritable(score));
		}
	}
	
}
