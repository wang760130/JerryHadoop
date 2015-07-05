package org.apache.hadoop.examples;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Join extends Configured implements Tool {
	static int printUsage() {
		System.out.println("join [-m <maps>] [-r <reduces>] [-inFormat <input format class>] [-outFormat <output format class>] [-outKey <output key class>] [-outValue <output value class>] [-joinOp <inner|outer|override>] [input]* <input> <output>");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(getConf(), Sort.class);
		jobConf.setJobName("join");

		jobConf.setMapperClass(IdentityMapper.class);
		jobConf.setReducerClass(IdentityReducer.class);

		JobClient client = new JobClient(jobConf);
		ClusterStatus cluster = client.getClusterStatus();
		int num_maps = cluster.getTaskTrackers()
				* jobConf.getInt("test.sort.maps_per_host", 10);

		int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9D);
		String sort_reduces = jobConf.get("test.sort.reduces_per_host");
		if (sort_reduces != null) {
			num_reduces = cluster.getTaskTrackers()
					* Integer.parseInt(sort_reduces);
		}

		Class inputFormatClass = SequenceFileInputFormat.class;

		Class outputFormatClass = SequenceFileOutputFormat.class;

		Class outputKeyClass = BytesWritable.class;
		Class outputValueClass = TupleWritable.class;
		String op = "inner";
		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			try {
				if ("-m".equals(args[i]))
					num_maps = Integer.parseInt(args[(++i)]);
				else if ("-r".equals(args[i]))
					num_reduces = Integer.parseInt(args[(++i)]);
				else if ("-inFormat".equals(args[i])) {
					inputFormatClass = Class.forName(args[(++i)]).asSubclass(
							InputFormat.class);
				} else if ("-outFormat".equals(args[i])) {
					outputFormatClass = Class.forName(args[(++i)]).asSubclass(
							OutputFormat.class);
				} else if ("-outKey".equals(args[i])) {
					outputKeyClass = Class.forName(args[(++i)]).asSubclass(
							WritableComparable.class);
				} else if ("-outValue".equals(args[i])) {
					outputValueClass = Class.forName(args[(++i)]).asSubclass(
							Writable.class);
				} else if ("-joinOp".equals(args[i]))
					op = args[(++i)];
				else
					otherArgs.add(args[i]);
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[(i - 1)]);

				return printUsage();
			}

		}

		jobConf.setNumMapTasks(num_maps);
		jobConf.setNumReduceTasks(num_reduces);

		if (otherArgs.size() < 2) {
			System.out.println("ERROR: Wrong number of parameters: ");
			return printUsage();
		}

		FileOutputFormat.setOutputPath(jobConf,
				new Path((String) otherArgs.remove(otherArgs.size() - 1)));

		List plist = new ArrayList(otherArgs.size());
		for (String s : otherArgs) {
			plist.add(new Path(s));
		}

		jobConf.setInputFormat(CompositeInputFormat.class);
		jobConf.set("mapred.join.expr", CompositeInputFormat.compose(op,
				inputFormatClass, (Path[]) plist.toArray(new Path[0])));

		jobConf.setOutputFormat(outputFormatClass);

		jobConf.setOutputKeyClass(outputKeyClass);
		jobConf.setOutputValueClass(outputValueClass);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(jobConf);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000L
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Join(), args);
		System.exit(res);
	}
}
