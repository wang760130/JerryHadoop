package com.jerry.hadoop.mapreducer.access.accessip;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jerry.hadoop.mapreducer.common.Global;

public class AccessIP {
	private static String name = "accessip";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = Global.getHdfsUrl() + "mapreducer/access/inputfile";
		String output = Global.getOutputFile(name);
		
		Configuration conf = new Configuration();
		args = new String[] {input, output};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, name);
		job.setJarByClass(AccessIP.class);
		
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
