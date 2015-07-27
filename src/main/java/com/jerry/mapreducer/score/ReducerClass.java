package com.jerry.mapreducer.score;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		int count = 0;
		
		Iterator<IntWritable> iterator = values.iterator();
		
		while(iterator.hasNext()) {
			// 計算總分
			sum += iterator.next().get();
			// 统计总的科目数
			count++;
		}
		
		// 计算平均成绩
		int average = (int) sum / count;
		context.write(key, new IntWritable(average));
	}
}
