package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class TeraInputFormat extends FileInputFormat<Text, Text> {
	static final String PARTITION_FILENAME = "_partition.lst";
	static final String SAMPLE_SIZE = "terasort.partitions.sample";
	private static JobConf lastConf = null;
	private static InputSplit[] lastResult = null;

	public static void writePartitionFile(JobConf conf, Path partFile)
			throws IOException {
		TeraInputFormat inFormat = new TeraInputFormat();
		TextSampler sampler = new TextSampler();
		Text key = new Text();
		Text value = new Text();
		int partitions = conf.getNumReduceTasks();
		long sampleSize = conf.getLong("terasort.partitions.sample", 100000L);
		InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
		int samples = Math.min(10, splits.length);
		long recordsPerSample = sampleSize / samples;
		int sampleStep = splits.length / samples;
		long records = 0L;

		for (int i = 0; i < samples; i++) {
			RecordReader reader = inFormat.getRecordReader(
					splits[(sampleStep * i)], conf, null);

			while (reader.next(key, value)) {
				sampler.addKey(key);
				records += 1L;
				if ((i + 1) * recordsPerSample <= records) {
					break;
				}
			}
		}
		FileSystem outFs = partFile.getFileSystem(conf);
		if (outFs.exists(partFile)) {
			outFs.delete(partFile, false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(outFs, conf,
				partFile, Text.class, NullWritable.class);

		NullWritable nullValue = NullWritable.get();
		for (Text split : sampler.createPartitions(partitions)) {
			writer.append(split, nullValue);
		}
		writer.close();
	}

	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return new TeraRecordReader(job, (FileSplit) split);
	}

	public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
		if (conf == lastConf) {
			return lastResult;
		}
		lastConf = conf;
		lastResult = super.getSplits(conf, splits);
		return lastResult;
	}

	static class TeraRecordReader implements RecordReader<Text, Text> {
		private LineRecordReader in;
		private LongWritable junk = new LongWritable();
		private Text line = new Text();
		private static int KEY_LENGTH = 10;

		public TeraRecordReader(Configuration job, FileSplit split)
				throws IOException {
			this.in = new LineRecordReader(job, split);
		}

		public void close() throws IOException {
			this.in.close();
		}

		public Text createKey() {
			return new Text();
		}

		public Text createValue() {
			return new Text();
		}

		public long getPos() throws IOException {
			return this.in.getPos();
		}

		public float getProgress() throws IOException {
			return this.in.getProgress();
		}

		public boolean next(Text key, Text value) throws IOException {
			if (this.in.next(this.junk, this.line)) {
				if (this.line.getLength() < KEY_LENGTH) {
					key.set(this.line);
					value.clear();
				} else {
					byte[] bytes = this.line.getBytes();
					key.set(bytes, 0, KEY_LENGTH);
					value.set(bytes, KEY_LENGTH, this.line.getLength()
							- KEY_LENGTH);
				}
				return true;
			}
			return false;
		}
	}

	static class TextSampler implements IndexedSortable {
		private ArrayList<Text> records = new ArrayList();

		public int compare(int i, int j) {
			Text left = (Text) this.records.get(i);
			Text right = (Text) this.records.get(j);
			return left.compareTo(right);
		}

		public void swap(int i, int j) {
			Text left = (Text) this.records.get(i);
			Text right = (Text) this.records.get(j);
			this.records.set(j, left);
			this.records.set(i, right);
		}

		public void addKey(Text key) {
			this.records.add(new Text(key));
		}

		Text[] createPartitions(int numPartitions) {
			int numRecords = this.records.size();
			System.out.println("Making " + numPartitions + " from "
					+ numRecords + " records");

			if (numPartitions > numRecords) {
				throw new IllegalArgumentException(
						"Requested more partitions than input keys ("
								+ numPartitions + " > " + numRecords + ")");
			}

			new QuickSort().sort(this, 0, this.records.size());
			float stepSize = numRecords / numPartitions;
			System.out.println("Step size is " + stepSize);
			Text[] result = new Text[numPartitions - 1];
			for (int i = 1; i < numPartitions; i++) {
				result[(i - 1)] = ((Text) this.records.get(Math.round(stepSize
						* i)));
			}
			return result;
		}
	}
}

/*
 * Location: F:\hadoop-1.1.2\hadoop-examples-1.1.2.jar Qualified Name:
 * org.apache.hadoop.examples.terasort.TeraInputFormat JD-Core Version: 0.6.2
 */
