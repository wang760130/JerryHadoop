package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomWriter extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Usage: writer <out-dir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path outDir = new Path(args[0]);
		JobConf job = new JobConf(getConf());

		job.setJarByClass(RandomWriter.class);
		job.setJobName("random-writer");
		FileOutputFormat.setOutputPath(job, outDir);

		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormat(RandomInputFormat.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		int numMapsPerHost = job.getInt("test.randomwriter.maps_per_host", 10);
		long numBytesToWritePerMap = job.getLong(
				"test.randomwrite.bytes_per_map", 1073741824L);

		if (numBytesToWritePerMap == 0L) {
			System.err
					.println("Cannot have test.randomwrite.bytes_per_map set to 0");
			return -2;
		}
		long totalBytesToWrite = job.getLong(
				"test.randomwrite.total_bytes",
				numMapsPerHost * numBytesToWritePerMap
						* cluster.getTaskTrackers());

		int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
		if ((numMaps == 0) && (totalBytesToWrite > 0L)) {
			numMaps = 1;
			job.setLong("test.randomwrite.bytes_per_map", totalBytesToWrite);
		}

		job.setNumMapTasks(numMaps);
		System.out.println("Running " + numMaps + " maps.");

		job.setNumReduceTasks(0);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(job);
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The job took "
				+ (endTime.getTime() - startTime.getTime()) / 1000L
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RandomWriter(), args);
		System.exit(res);
	}

	static class Map extends MapReduceBase implements
			Mapper<WritableComparable, Writable, BytesWritable, BytesWritable> {
		private long numBytesToWrite;
		private int minKeySize;
		private int keySizeRange;
		private int minValueSize;
		private int valueSizeRange;
		private Random random = new Random();
		private BytesWritable randomKey = new BytesWritable();
		private BytesWritable randomValue = new BytesWritable();

		private void randomizeBytes(byte[] data, int offset, int length) {
			for (int i = offset + length - 1; i >= offset; i--)
				data[i] = ((byte) this.random.nextInt(256));
		}

		public void map(WritableComparable key, Writable value,
				OutputCollector<BytesWritable, BytesWritable> output,
				Reporter reporter) throws IOException {
			int itemCount = 0;
			while (this.numBytesToWrite > 0L) {
				int keyLength = this.minKeySize
						+ (this.keySizeRange != 0 ? this.random
								.nextInt(this.keySizeRange) : 0);

				this.randomKey.setSize(keyLength);
				randomizeBytes(this.randomKey.getBytes(), 0,
						this.randomKey.getLength());
				int valueLength = this.minValueSize
						+ (this.valueSizeRange != 0 ? this.random
								.nextInt(this.valueSizeRange) : 0);

				this.randomValue.setSize(valueLength);
				randomizeBytes(this.randomValue.getBytes(), 0,
						this.randomValue.getLength());
				output.collect(this.randomKey, this.randomValue);
				this.numBytesToWrite -= keyLength + valueLength;
				reporter.incrCounter(RandomWriter.Counters.BYTES_WRITTEN,
						keyLength + valueLength);
				reporter.incrCounter(RandomWriter.Counters.RECORDS_WRITTEN, 1L);
				itemCount++;
				if (itemCount % 200 == 0) {
					reporter.setStatus("wrote record " + itemCount + ". "
							+ this.numBytesToWrite + " bytes left.");
				}
			}

			reporter.setStatus("done with " + itemCount + " records.");
		}

		public void configure(JobConf job) {
			this.numBytesToWrite = job.getLong(
					"test.randomwrite.bytes_per_map", 1073741824L);

			this.minKeySize = job.getInt("test.randomwrite.min_key", 10);
			this.keySizeRange = (job.getInt("test.randomwrite.max_key", 1000) - this.minKeySize);

			this.minValueSize = job.getInt("test.randomwrite.min_value", 0);
			this.valueSizeRange = (job.getInt("test.randomwrite.max_value",
					20000) - this.minValueSize);
		}
	}

	static class RandomInputFormat implements InputFormat<Text, Text> {
		public InputSplit[] getSplits(JobConf job, int numSplits)
				throws IOException {
			InputSplit[] result = new InputSplit[numSplits];
			Path outDir = FileOutputFormat.getOutputPath(job);
			for (int i = 0; i < result.length; i++) {
				result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i),
						0L, 1L, (String[]) null);
			}

			return result;
		}

		public RecordReader<Text, Text> getRecordReader(InputSplit split,
				JobConf job, Reporter reporter) throws IOException {
			return new RandomRecordReader(((FileSplit) split).getPath());
		}

		static class RandomRecordReader implements RecordReader<Text, Text> {
			Path name;

			public RandomRecordReader(Path p) {
				this.name = p;
			}

			public boolean next(Text key, Text value) {
				if (this.name != null) {
					key.set(this.name.getName());
					this.name = null;
					return true;
				}
				return false;
			}

			public Text createKey() {
				return new Text();
			}

			public Text createValue() {
				return new Text();
			}

			public long getPos() {
				return 0L;
			}

			public void close() {
			}

			public float getProgress() {
				return 0.0F;
			}

		}
	}

	static enum Counters {
		RECORDS_WRITTEN, BYTES_WRITTEN;
	}
}
