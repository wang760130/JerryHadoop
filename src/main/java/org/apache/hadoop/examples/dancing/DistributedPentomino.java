package org.apache.hadoop.examples.dancing;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedPentomino extends Configured implements Tool {
	private static void createInputDirectory(FileSystem fs, Path dir,
			Pentomino pent, int depth) throws IOException {
		fs.mkdirs(dir);
		List<int[]> splits = pent.getSplits(depth);
		PrintStream file = new PrintStream(new BufferedOutputStream(
				fs.create(new Path(dir, "part1")), 65536));

		for (int[] prefix : splits) {
			for (int i = 0; i < prefix.length; i++) {
				if (i != 0) {
					file.print(',');
				}
				file.print(prefix[i]);
			}
			file.print('\n');
		}
		file.close();
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new DistributedPentomino(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		int depth = 5;
		int width = 9;
		int height = 10;

		if (args.length == 0) {
			System.out.println("pentomino <output>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		JobConf conf = new JobConf(getConf());
		width = conf.getInt("pent.width", width);
		height = conf.getInt("pent.height", height);
		depth = conf.getInt("pent.depth", depth);
		Class pentClass = conf.getClass("pent.class", OneSidedPentomino.class,
				Pentomino.class);

		Path output = new Path(args[0]);
		Path input = new Path(output + "_input");
		FileSystem fileSys = FileSystem.get(conf);
		try {
			FileInputFormat.setInputPaths(conf, new Path[] { input });
			FileOutputFormat.setOutputPath(conf, output);
			conf.setJarByClass(PentMap.class);

			conf.setJobName("dancingElephant");
			Pentomino pent = (Pentomino) ReflectionUtils.newInstance(pentClass,
					conf);
			pent.initialize(width, height);
			createInputDirectory(fileSys, input, pent, depth);

			conf.setOutputKeyClass(Text.class);

			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(PentMap.class);
			conf.setReducerClass(IdentityReducer.class);

			conf.setNumMapTasks(2000);
			conf.setNumReduceTasks(1);

			JobClient.runJob(conf);
		} finally {
			fileSys.delete(input, true);
		}
		return 0;
	}

	public static class PentMap extends MapReduceBase implements
			Mapper<WritableComparable, Text, Text, Text> {
		private int width;
		private int height;
		private int depth;
		private Pentomino pent;
		private Text prefixString;
		private OutputCollector<Text, Text> output;
		private Reporter reporter;

		public void map(WritableComparable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			this.output = output;
			this.reporter = reporter;
			this.prefixString = value;
			StringTokenizer itr = new StringTokenizer(
					this.prefixString.toString(), ",");
			int[] prefix = new int[this.depth];
			int idx = 0;
			while (itr.hasMoreTokens()) {
				String num = itr.nextToken();
				prefix[(idx++)] = Integer.parseInt(num);
			}
			this.pent.solve(prefix);
		}

		public void configure(JobConf conf) {
			this.depth = conf.getInt("pent.depth", -1);
			this.width = conf.getInt("pent.width", -1);
			this.height = conf.getInt("pent.height", -1);
			this.pent = ((Pentomino) ReflectionUtils.newInstance(
					conf.getClass("pent.class", OneSidedPentomino.class), conf));

			this.pent.initialize(this.width, this.height);
			this.pent.setPrinter(new SolutionCatcher());
		}

		class SolutionCatcher implements
				DancingLinks.SolutionAcceptor<Pentomino.ColumnName> {
			SolutionCatcher() {
			}

			public void solution(List<List<Pentomino.ColumnName>> answer) {
				String board = Pentomino.stringifySolution(
						DistributedPentomino.PentMap.this.width,
						DistributedPentomino.PentMap.this.height, answer);
				try {
					DistributedPentomino.PentMap.this.output.collect(
							DistributedPentomino.PentMap.this.prefixString,
							new Text("\n" + board));
					DistributedPentomino.PentMap.this.reporter.incrCounter(
							DistributedPentomino.PentMap.this.pent
									.getCategory(answer), 1L);
				} catch (IOException e) {
					System.err.println(StringUtils.stringifyException(e));
				}
			}
		}
	}
}
