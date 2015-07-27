package com.jerry.mapreduce.score;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jerry.mapreduce.common.Global;

public class Score {
	private static String name = "score";
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = Global.getInputFile(name);
		String output = Global.getOutputFile(name);
		
		Configuration conf = new Configuration();
		
        String[] ioArgs = new String[] {input, output};
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        
        Job job = new Job(conf, name);
        
        job.setJarByClass(Score.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
