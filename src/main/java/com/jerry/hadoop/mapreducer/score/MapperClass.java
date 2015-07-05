package com.jerry.hadoop.mapreducer.score;

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
		// 将输入的纯文本文件的数据转化成String
		String line = value.toString();
		
		// 将输入的数据首先按行进行分割
		StringTokenizer tokenizerArticle  = new StringTokenizer(line,"\n");
		
		// 分别对每一行进行处理
		while(tokenizerArticle .hasMoreElements()) {
			// 每行按空格划分
			StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
			
			// 学生姓名部分
			String nameStr = tokenizerLine.nextToken();

			// 成绩部分
            String scoreStr = tokenizerLine.nextToken();
            Text name = new Text(nameStr);
            int score = Integer.parseInt(scoreStr);
            
            // 输出姓名和成绩
            context.write(name, new IntWritable(score));
		}
	}
	
}
