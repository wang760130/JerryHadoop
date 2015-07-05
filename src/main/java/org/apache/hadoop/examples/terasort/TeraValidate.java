package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraValidate extends Configured implements Tool {
	private static final Text error = new Text("error");

	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		TeraInputFormat.setInputPaths(job, new Path[] { new Path(args[0]) });
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJobName("TeraValidate");
		job.setJarByClass(TeraValidate.class);
		job.setMapperClass(ValidateMapper.class);
		job.setReducerClass(ValidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		job.setLong("mapred.min.split.size", 9223372036854775807L);
		job.setInputFormat(TeraInputFormat.class);
		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new TeraValidate(), args);
		System.exit(res);
	}

	static class ValidateReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private boolean firstKey = true;
		private Text lastKey = new Text();
		private Text lastValue = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (TeraValidate.error.equals(key)) {
				while (values.hasNext()) {
					output.collect(key, values.next());
				}
			}
			Text value = (Text) values.next();
			if (this.firstKey) {
				this.firstKey = false;
			} else if (value.compareTo(this.lastValue) < 0) {
				output.collect(TeraValidate.error, new Text(
						"misordered keys last: " + this.lastKey + " '"
								+ this.lastValue + "' current: " + key + " '"
								+ value + "'"));
			}

			this.lastKey.set(key);
			this.lastValue.set(value);
		}
	}

	static class ValidateMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private Text lastKey;
		private OutputCollector<Text, Text> output;
		private String filename;

		private String getFilename(FileSplit split) {
			return split.getPath().getName();
		}

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (this.lastKey == null) {
				this.filename = getFilename((FileSplit) reporter
						.getInputSplit());
				output.collect(new Text(this.filename + ":begin"), key);
				this.lastKey = new Text();
				this.output = output;
			} else if (key.compareTo(this.lastKey) < 0) {
				output.collect(TeraValidate.error, new Text("misorder in "
						+ this.filename + " last: '" + this.lastKey
						+ "' current: '" + key + "'"));
			}

			this.lastKey.set(key);
		}

		public void close() throws IOException {
			if (this.lastKey != null)
				this.output.collect(new Text(this.filename + ":end"),
						this.lastKey);
		}
	}
}

