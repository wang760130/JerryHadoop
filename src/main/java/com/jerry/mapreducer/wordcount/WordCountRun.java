package com.jerry.mapreducer.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jerry.mapreducer.common.Global;

public class WordCountRun extends Configured implements Tool {
	private static String name = "wordcount";
	enum Counter {
		LINESKIP       // 出错的行
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		String input = Global.getInputFile(name);
		String output = Global.getOutputFile(name);
		
		Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(WordCount.class);  
        job.setJobName(name);  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        job.setMapperClass(MapClass.class);  
        job.setReducerClass(ReducerClass.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return(job.waitForCompletion(true) ? 1 : 0);
	}
	
	public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);
			try {
				// 获取值
				StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
				while(stringTokenizer.hasMoreTokens()) {
					word.set(stringTokenizer.nextToken());
					context.write(word, one); 
				}
			} catch (Exception e) {
				context.getCounter(Counter.LINESKIP).increment(1);
				return ;
			}
		}
	}
	
	public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			IntWritable result = new IntWritable();
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new WordCountRun(), args);
		System.exit(result);
	}
}
