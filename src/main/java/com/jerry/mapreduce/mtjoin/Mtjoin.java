package com.jerry.mapreduce.mtjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jerry.mapreduce.common.Global;

/**
 * 多表关联和单表关联相似，都类似于数据库中的自然连接。
 * 相比单表关联，多表关联的左右表和连接列更加清楚。
 * 所以可以采用和单表关联的相同的处理方式，map识别出输入的行属于哪个表之后，
 * 对其进行分割，将连接的列值保存在key中，另一列和左右表标识保存在value中，然后输出。
 * reduce拿到连接结果之后，解析value内容，根据标志将左右表内容分开存放，
 * 然后求笛卡尔积，最后直接输出。
*/
public class Mtjoin {
	private static String name = "mtjoin";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = Global.getInputFile(name);
		String output = Global.getOutputFile(name);
		
		Configuration conf = new Configuration();
		args = new String[] {input, output};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, name);
		job.setJarByClass(Mtjoin.class);
		
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
