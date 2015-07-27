package com.jerry.mapreduce.stjoin;

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
 * 要用MapReduce解决这个实例，首先应该考虑如何实现表的自连接；
 * 其次就是连接列的设置；最后是结果的整理。
 * 考虑到MapReduce的shuffle过程会将相同的key会连接在一起，
 * 所以可以将map结果的key设置成待连接的列，然后列中相同的值就自然会连接在一起了。
 * 再与最开始的分析联系起来：
 * 要连接的是左表的parent列和右表的child列，且左表和右表是同一个表，
 * 所以在map阶段将读入数据分割成child和parent之后，会将parent设置成key，
 * child设置成value进行输出，并作为左表；再将同一对child和parent中的child设置成key，
 * parent设置成value进行输出，作为右表。为了区分输出中的左右表，
 * 需要在输出的value中再加上左右表的信息，比如在value的String最开始处加上字符1表示左表，
 * 加上字符2表示右表。这样在map的结果中就形成了左表和右表，然后在shuffle过程中完成连接。
 * reduce接收到连接的结果，其中每个key的value-list就包含了"grandchild--grandparent"关系。
 * 取出每个key的value-list进行解析，将左表中的child放入一个数组，右表中的parent放入一个数组，
 * 然后对两个数组求笛卡尔积就是最后的结果了。
 * 
 * @author JerryWang
 *
 */
public class Stjoin {
	private static String name = "stjoin";
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		String input = Global.getInputFile(name);
		String output = Global.getOutputFile(name);
		
		Configuration conf = new Configuration();
		args = new String[] {input, output};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, name);
		job.setJarByClass(Stjoin.class);
		
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
