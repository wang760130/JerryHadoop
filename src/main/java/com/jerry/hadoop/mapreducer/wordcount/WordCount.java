package com.jerry.hadoop.mapreducer.wordcount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author JerryWang
 */
public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = "hdfs://10.58.29.85:9000/mapreducer/wordcount/inputfile";
		String output = "hdfs://10.58.29.85:9000/mapreducer/wordcount/outputfile";
		
		Configuration conf = new Configuration();
		args = new String[] {input, output};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2) {
			System.out.println("Usage : wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setCombinerClass(ReducerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
