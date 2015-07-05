package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiFileWordCount extends Configured implements Tool {
	private void printUsage() {
		System.out.println("Usage : multifilewc <input_dir> <output>");
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			return 1;
		}

		JobConf job = new JobConf(getConf(), MultiFileWordCount.class);
		job.setJobName("MultiFileWordCount");

		job.setInputFormat(MyInputFormat.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(MapClass.class);

		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MultiFileWordCount(), args);
		System.exit(ret);
	}

	public static class MapClass extends MapReduceBase implements
			Mapper<MultiFileWordCount.WordOffset, Text, Text, LongWritable> {
		private static final LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		public void map(MultiFileWordCount.WordOffset key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				this.word.set(itr.nextToken());
				output.collect(this.word, one);
			}
		}
	}

	public static class MultiFileLineRecordReader implements
			RecordReader<MultiFileWordCount.WordOffset, Text> {
		private MultiFileSplit split;
		private long offset;
		private long totLength;
		private FileSystem fs;
		private int count = 0;
		private Path[] paths;
		private FSDataInputStream currentStream;
		private BufferedReader currentReader;

		public MultiFileLineRecordReader(Configuration conf,
				MultiFileSplit split) throws IOException {
			this.split = split;
			this.fs = FileSystem.get(conf);
			this.paths = split.getPaths();
			this.totLength = split.getLength();
			this.offset = 0L;

			Path file = this.paths[this.count];
			this.currentStream = this.fs.open(file);
			this.currentReader = new BufferedReader(new InputStreamReader(
					this.currentStream));
		}

		public void close() throws IOException {
		}

		public long getPos() throws IOException {
			long currentOffset = this.currentStream == null ? 0L
					: this.currentStream.getPos();
			return this.offset + currentOffset;
		}

		public float getProgress() throws IOException {
			return (float) getPos() / (float) this.totLength;
		}

		public boolean next(MultiFileWordCount.WordOffset key, Text value)
				throws IOException {
			if (this.count >= this.split.getNumPaths()) {
				return false;
			}

			String line;
			do {
				line = this.currentReader.readLine();
				if (line == null) {
					this.currentReader.close();
					this.offset += this.split.getLength(this.count);

					if (++this.count >= this.split.getNumPaths()) {
						return false;
					}

					Path file = this.paths[this.count];
					this.currentStream = this.fs.open(file);
					this.currentReader = new BufferedReader(
							new InputStreamReader(this.currentStream));
//					MultiFileWordCount.WordOffset.access$002(key,
//							file.getName());
				}
			} while (line == null);

//			MultiFileWordCount.WordOffset.access$102(key,
//					this.currentStream.getPos());
			value.set(line);

			return true;
		}

		public MultiFileWordCount.WordOffset createKey() {
			MultiFileWordCount.WordOffset wo = new MultiFileWordCount.WordOffset();
//			MultiFileWordCount.WordOffset.access$002(wo,
//					this.paths[0].toString());
			return wo;
		}

		public Text createValue() {
			return new Text();
		}
	}

	public static class MyInputFormat extends
			MultiFileInputFormat<MultiFileWordCount.WordOffset, Text> {
		public RecordReader<MultiFileWordCount.WordOffset, Text> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new MultiFileWordCount.MultiFileLineRecordReader(job,
					(MultiFileSplit) split);
		}
	}

	public static class WordOffset implements WritableComparable {
		private long offset;
		private String fileName;

		public void readFields(DataInput in) throws IOException {
			this.offset = in.readLong();
			this.fileName = Text.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(this.offset);
			Text.writeString(out, this.fileName);
		}

		public int compareTo(Object o) {
			WordOffset that = (WordOffset) o;

			int f = this.fileName.compareTo(that.fileName);
			if (f == 0) {
				return (int) Math.signum(this.offset - that.offset);
			}
			return f;
		}

		public boolean equals(Object obj) {
			if ((obj instanceof WordOffset))
				return compareTo(obj) == 0;
			return false;
		}

		public int hashCode() {
//			if (!$assertionsDisabled)
//				throw new AssertionError("hashCode not designed");
			return 42;
		}
	}
}
