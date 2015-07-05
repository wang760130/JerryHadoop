package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SleepJob extends Configured implements Tool,
		Mapper<IntWritable, IntWritable, IntWritable, NullWritable>,
		Reducer<IntWritable, NullWritable, NullWritable, NullWritable>,
		Partitioner<IntWritable, NullWritable> {
	private long mapSleepDuration;
	private long reduceSleepDuration;
	private int mapSleepCount;
	private int reduceSleepCount;
	private int count;

	public SleepJob() {
		this.mapSleepDuration = 100L;
		this.reduceSleepDuration = 100L;
		this.mapSleepCount = 1;
		this.reduceSleepCount = 1;
		this.count = 0;
	}

	public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
		return k.get() % numPartitions;
	}

	public void map(IntWritable key, IntWritable value,
			OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
			throws IOException {
		try {
			reporter.setStatus("Sleeping... (" + this.mapSleepDuration
					* (this.mapSleepCount - this.count) + ") ms left");

			Thread.sleep(this.mapSleepDuration);
		} catch (InterruptedException ex) {
			throw ((IOException) new IOException("Interrupted while sleeping")
					.initCause(ex));
		}

		this.count += 1;

		int k = key.get();
		for (int i = 0; i < value.get(); i++)
			output.collect(new IntWritable(k + i), NullWritable.get());
	}

	public void reduce(IntWritable key, Iterator<NullWritable> values,
			OutputCollector<NullWritable, NullWritable> output,
			Reporter reporter) throws IOException {
		try {
			reporter.setStatus("Sleeping... (" + this.reduceSleepDuration
					* (this.reduceSleepCount - this.count) + ") ms left");

			Thread.sleep(this.reduceSleepDuration);
		} catch (InterruptedException ex) {
			throw ((IOException) new IOException("Interrupted while sleeping")
					.initCause(ex));
		}

		this.count += 1;
	}

	public void configure(JobConf job) {
		this.mapSleepCount = job.getInt("sleep.job.map.sleep.count",
				this.mapSleepCount);

		this.reduceSleepCount = job.getInt("sleep.job.reduce.sleep.count",
				this.reduceSleepCount);

		this.mapSleepDuration = (job.getLong("sleep.job.map.sleep.time", 100L) / this.mapSleepCount);

		this.reduceSleepDuration = (job.getLong("sleep.job.reduce.sleep.time",
				100L) / this.reduceSleepCount);
	}

	public void close() throws IOException {
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SleepJob(), args);
		System.exit(res);
	}

	public int run(int numMapper, int numReducer, long mapSleepTime,
			int mapSleepCount, long reduceSleepTime, int reduceSleepCount)
			throws IOException {
		JobConf job = setupJobConf(numMapper, numReducer, mapSleepTime,
				mapSleepCount, reduceSleepTime, reduceSleepCount);

		JobClient.runJob(job);
		return 0;
	}

	public JobConf setupJobConf(int numMapper, int numReducer,
			long mapSleepTime, int mapSleepCount, long reduceSleepTime,
			int reduceSleepCount) {
		JobConf job = new JobConf(getConf(), SleepJob.class);
		job.setNumMapTasks(numMapper);
		job.setNumReduceTasks(numReducer);
		job.setMapperClass(SleepJob.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(SleepJob.class);
		job.setOutputFormat(NullOutputFormat.class);
		job.setInputFormat(SleepInputFormat.class);
		job.setPartitionerClass(SleepJob.class);
		job.setSpeculativeExecution(false);
		job.setJobName("Sleep job");
		FileInputFormat.addInputPath(job, new Path("ignored"));
		job.setLong("sleep.job.map.sleep.time", mapSleepTime);
		job.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);
		job.setInt("sleep.job.map.sleep.count", mapSleepCount);
		job.setInt("sleep.job.reduce.sleep.count", reduceSleepCount);
		return job;
	}

	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err
					.println("SleepJob [-m numMapper] [-r numReducer] [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)] [-recordt recordSleepTime (msec)]");

			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int numMapper = 1;
		int numReducer = 1;
		long mapSleepTime = 100L;
		long reduceSleepTime = 100L;
		long recSleepTime = 100L;
		int mapSleepCount = 1;
		int reduceSleepCount = 1;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-m")) {
				numMapper = Integer.parseInt(args[(++i)]);
			} else if (args[i].equals("-r")) {
				numReducer = Integer.parseInt(args[(++i)]);
			} else if (args[i].equals("-mt")) {
				mapSleepTime = Long.parseLong(args[(++i)]);
			} else if (args[i].equals("-rt")) {
				reduceSleepTime = Long.parseLong(args[(++i)]);
			} else if (args[i].equals("-recordt")) {
				recSleepTime = Long.parseLong(args[(++i)]);
			}

		}

		mapSleepCount = (int) Math.ceil(mapSleepTime / recSleepTime);
		reduceSleepCount = (int) Math.ceil(reduceSleepTime / recSleepTime);

		return run(numMapper, numReducer, mapSleepTime, mapSleepCount,
				reduceSleepTime, reduceSleepCount);
	}

	public static class SleepInputFormat extends Configured implements
			InputFormat<IntWritable, IntWritable> {
		public InputSplit[] getSplits(JobConf conf, int numSplits) {
			InputSplit[] ret = new InputSplit[numSplits];
			for (int i = 0; i < numSplits; i++) {
				ret[i] = new SleepJob.EmptySplit();
			}
			return ret;
		}

		public RecordReader<IntWritable, IntWritable> getRecordReader(
				InputSplit ignored, JobConf conf, Reporter reporter)
				throws IOException {
			final int count = conf.getInt("sleep.job.map.sleep.count", 1);
			if (count < 0)
				throw new IOException("Invalid map count: " + count);
			int redcount = conf.getInt("sleep.job.reduce.sleep.count", 1);
			if (redcount < 0)
				throw new IOException("Invalid reduce count: " + redcount);
			final int emitPerMapTask = redcount * conf.getNumReduceTasks();
			return new RecordReader() {
				private int records = 0;
				private int emitCount = 0;

				public boolean next(IntWritable key, IntWritable value)
						throws IOException {
					key.set(this.emitCount);
					int emit = emitPerMapTask / count;
					if (emitPerMapTask % count > this.records) {
						emit++;
					}
					this.emitCount += emit;
					value.set(emit);
					return this.records++ < count;
				}

				public IntWritable createKey() {
					return new IntWritable();
				}

				public IntWritable createValue() {
					return new IntWritable();
				}

				public long getPos() throws IOException {
					return this.records;
				}

				public void close() throws IOException {
				}

				public float getProgress() throws IOException {
					return this.records / count;
				}

				@Override
				public boolean next(Object key, Object value)
						throws IOException {
					// TODO Auto-generated method stub
					return false;
				}

			};
		}
	}

	public static class EmptySplit implements InputSplit {
		public void write(DataOutput out) throws IOException {
		}

		public void readFields(DataInput in) throws IOException {
		}

		public long getLength() {
			return 0L;
		}

		public String[] getLocations() {
			return new String[0];
		}

	}
}
