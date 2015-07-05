package org.apache.hadoop.examples.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraGen extends Configured implements Tool {
	static long getNumberOfRows(JobConf job) {
		return job.getLong("terasort.num-rows", 0L);
	}

	static void setNumberOfRows(JobConf job, long numRows) {
		job.setLong("terasort.num-rows", numRows);
	}

	public int run(String[] args) throws IOException {
		JobConf job = (JobConf) getConf();
		setNumberOfRows(job, Long.parseLong(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJobName("TeraGen");
		job.setJarByClass(TeraGen.class);
		job.setMapperClass(SortGenMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(RangeInputFormat.class);
		job.setOutputFormat(TeraOutputFormat.class);
		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new TeraGen(), args);
		System.exit(res);
	}

	public static class SortGenMapper extends MapReduceBase implements
			Mapper<LongWritable, NullWritable, Text, Text> {
		private Text key;
		private Text value;
		private TeraGen.RandomGenerator rand;
		private byte[] keyBytes;
		private byte[] spaces;
		private byte[][] filler;

		public SortGenMapper() {
			this.key = new Text();
			this.value = new Text();

			this.keyBytes = new byte[12];
			this.spaces = "          ".getBytes();
			this.filler = new byte[26][];

			for (int i = 0; i < 26; i++) {
				this.filler[i] = new byte[10];
				for (int j = 0; j < 10; j++)
					this.filler[i][j] = ((byte) (65 + i));
			}
		}

		private void addKey() {
			for (int i = 0; i < 3; i++) {
				long temp = this.rand.next() / 52L;
				this.keyBytes[(3 + 4 * i)] = ((byte) (int) (32L + temp % 95L));
				temp /= 95L;
				this.keyBytes[(2 + 4 * i)] = ((byte) (int) (32L + temp % 95L));
				temp /= 95L;
				this.keyBytes[(1 + 4 * i)] = ((byte) (int) (32L + temp % 95L));
				temp /= 95L;
				this.keyBytes[(4 * i)] = ((byte) (int) (32L + temp % 95L));
			}
			this.key.set(this.keyBytes, 0, 10);
		}

		private void addRowId(long rowId) {
			byte[] rowid = Integer.toString((int) rowId).getBytes();
			int padSpace = 10 - rowid.length;
			if (padSpace > 0) {
				this.value.append(this.spaces, 0, 10 - rowid.length);
			}
			this.value.append(rowid, 0, Math.min(rowid.length, 10));
		}

		private void addFiller(long rowId) {
			int base = (int) (rowId * 8L % 26L);
			for (int i = 0; i < 7; i++) {
				this.value.append(this.filler[((base + i) % 26)], 0, 10);
			}
			this.value.append(this.filler[((base + 7) % 26)], 0, 8);
		}

		public void map(LongWritable row, NullWritable ignored,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long rowId = row.get();
			if (this.rand == null) {
				this.rand = new TeraGen.RandomGenerator(rowId * 3L);
			}
			addKey();
			this.value.clear();
			addRowId(rowId);
			addFiller(rowId);
			output.collect(this.key, this.value);
		}
	}

	static class RandomGenerator {
		private long seed = 0L;
		private static final long mask32 = 4294967295L;
		private static final int seedSkip = 134217728;
		private static final long[] seeds = { 0L, 4160749568L, 4026531840L,
				3892314112L, 3758096384L, 3623878656L, 3489660928L,
				3355443200L, 3221225472L, 3087007744L, 2952790016L,
				2818572288L, 2684354560L, 2550136832L, 2415919104L,
				2281701376L, 2147483648L, 2013265920L, 1879048192L,
				1744830464L, 1610612736L, 1476395008L, 1342177280L,
				1207959552L, 1073741824L, 939524096L, 805306368L, 671088640L,
				536870912L, 402653184L, 268435456L, 134217728L };

		RandomGenerator(long initalIteration) {
			int baseIndex = (int) ((initalIteration & 0xFFFFFFFF) / 134217728L);
			this.seed = seeds[baseIndex];
			for (int i = 0; i < initalIteration % 134217728L; i++)
				next();
		}

		RandomGenerator() {
			this(0L);
		}

		long next() {
			this.seed = (this.seed * 3141592621L + 663896637L & 0xFFFFFFFF);
			return this.seed;
		}
	}

	static class RangeInputFormat implements
			InputFormat<LongWritable, NullWritable> {
		public RecordReader<LongWritable, NullWritable> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new RangeRecordReader((RangeInputSplit) split);
		}

		public InputSplit[] getSplits(JobConf job, int numSplits) {
			long totalRows = TeraGen.getNumberOfRows(job);
			long rowsPerSplit = totalRows / numSplits;
			System.out.println("Generating " + totalRows + " using "
					+ numSplits + " maps with step of " + rowsPerSplit);

			InputSplit[] splits = new InputSplit[numSplits];
			long currentRow = 0L;
			for (int split = 0; split < numSplits - 1; split++) {
				splits[split] = new RangeInputSplit(currentRow, rowsPerSplit);
				currentRow += rowsPerSplit;
			}
			splits[(numSplits - 1)] = new RangeInputSplit(currentRow, totalRows
					- currentRow);

			return splits;
		}

		static class RangeRecordReader implements
				RecordReader<LongWritable, NullWritable> {
			long startRow;
			long finishedRows;
			long totalRows;

			public RangeRecordReader(
					TeraGen.RangeInputFormat.RangeInputSplit split) {
				this.startRow = split.firstRow;
				this.finishedRows = 0L;
				this.totalRows = split.rowCount;
			}

			public void close() throws IOException {
			}

			public LongWritable createKey() {
				return new LongWritable();
			}

			public NullWritable createValue() {
				return NullWritable.get();
			}

			public long getPos() throws IOException {
				return this.finishedRows;
			}

			public float getProgress() throws IOException {
				return (float) this.finishedRows / (float) this.totalRows;
			}

			public boolean next(LongWritable key, NullWritable value) {
				if (this.finishedRows < this.totalRows) {
					key.set(this.startRow + this.finishedRows);
					this.finishedRows += 1L;
					return true;
				}
				return false;
			}
		}

		static class RangeInputSplit implements InputSplit {
			long firstRow;
			long rowCount;

			public RangeInputSplit() {
			}

			public RangeInputSplit(long offset, long length) {
				this.firstRow = offset;
				this.rowCount = length;
			}

			public long getLength() throws IOException {
				return 0L;
			}

			public String[] getLocations() throws IOException {
				return new String[0];
			}

			public void readFields(DataInput in) throws IOException {
				this.firstRow = WritableUtils.readVLong(in);
				this.rowCount = WritableUtils.readVLong(in);
			}

			public void write(DataOutput out) throws IOException {
				WritableUtils.writeVLong(out, this.firstRow);
				WritableUtils.writeVLong(out, this.rowCount);
			}
		}
	}
}

/*
 * Location: F:\hadoop-1.1.2\hadoop-examples-1.1.2.jar Qualified Name:
 * org.apache.hadoop.examples.terasort.TeraGen JD-Core Version: 0.6.2
 */
