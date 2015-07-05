package com.jerry.hadoop.mapreducer.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 排序
 * @author JerryWang
 *
 */
public class Sort {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		args = new String[] {"/hadoop/mapreducer/sort/inputfile", "/hadoop/mapreducer/sort/outputfile" };
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Data Sort <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "sort");
		job.setJarByClass(Sort.class);

		// 设置Map和Reduce处理类
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);

		// 设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
