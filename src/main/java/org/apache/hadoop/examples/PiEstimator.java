package org.apache.hadoop.examples;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PiEstimator extends Configured implements Tool {
	private static final Path TMP_DIR = new Path(
			PiEstimator.class.getSimpleName() + "_TMP_3_141592654");

	public static BigDecimal estimate(int numMaps, long numPoints,
			JobConf jobConf) throws IOException {
		jobConf.setJobName(PiEstimator.class.getSimpleName());

		jobConf.setInputFormat(SequenceFileInputFormat.class);

		jobConf.setOutputKeyClass(BooleanWritable.class);
		jobConf.setOutputValueClass(LongWritable.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		jobConf.setMapperClass(PiMapper.class);
		jobConf.setNumMapTasks(numMaps);

		jobConf.setReducerClass(PiReducer.class);
		jobConf.setNumReduceTasks(1);

		jobConf.setSpeculativeExecution(false);

		Path inDir = new Path(TMP_DIR, "in");
		Path outDir = new Path(TMP_DIR, "out");
		FileInputFormat.setInputPaths(jobConf, new Path[] { inDir });
		FileOutputFormat.setOutputPath(jobConf, outDir);

		FileSystem fs = FileSystem.get(jobConf);
		if (fs.exists(TMP_DIR)) {
			throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
					+ " already exists.  Please remove it first.");
		}

		if (!fs.mkdirs(inDir)) {
			throw new IOException("Cannot create input directory " + inDir);
		}

		try {
			for (int i = 0; i < numMaps; i++) {
				Path file = new Path(inDir, "part" + i);
				LongWritable offset = new LongWritable(i * numPoints);
				LongWritable size = new LongWritable(numPoints);
				SequenceFile.Writer writer = SequenceFile.createWriter(fs,
						jobConf, file, LongWritable.class, LongWritable.class,
						SequenceFile.CompressionType.NONE);
				try {
					writer.append(offset, size);
				} finally {
					writer.close();
				}
				System.out.println("Wrote input for Map #" + i);
			}

			System.out.println("Starting Job");
			long startTime = System.currentTimeMillis();
			JobClient.runJob(jobConf);
			double duration = (System.currentTimeMillis() - startTime) / 1000.0D;
			System.out.println("Job Finished in " + duration + " seconds");

			Path inFile = new Path(outDir, "reduce-out");
			LongWritable numInside = new LongWritable();
			LongWritable numOutside = new LongWritable();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile,
					jobConf);
			try {
			} finally {
				reader.close();
			}

			return BigDecimal.valueOf(4L).setScale(20)
					.multiply(BigDecimal.valueOf(numInside.get()))
					.divide(BigDecimal.valueOf(numMaps))
					.divide(BigDecimal.valueOf(numPoints));
		} finally {
			fs.delete(TMP_DIR, true);
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: " + getClass().getName()
					+ " <nMaps> <nSamples>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int nMaps = Integer.parseInt(args[0]);
		long nSamples = Long.parseLong(args[1]);

		System.out.println("Number of Maps  = " + nMaps);
		System.out.println("Samples per Map = " + nSamples);

		JobConf jobConf = new JobConf(getConf(), getClass());
		System.out.println("Estimated value of Pi is "
				+ estimate(nMaps, nSamples, jobConf));

		return 0;
	}

	public static void main(String[] argv) throws Exception {
		System.exit(ToolRunner.run(null, new PiEstimator(), argv));
	}

	public static class PiReducer extends MapReduceBase
			implements
			Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable> {
		private long numInside = 0L;
		private long numOutside = 0L;
		private JobConf conf;

		public void configure(JobConf job) {
			this.conf = job;
		}

		public void reduce(BooleanWritable isInside,
				Iterator<LongWritable> values,
				OutputCollector<WritableComparable<?>, Writable> output,
				Reporter reporter) throws IOException {
			if (isInside.get()) {
				while (values.hasNext())
					this.numInside += ((LongWritable) values.next()).get();
			}
			while (values.hasNext())
				this.numOutside += ((LongWritable) values.next()).get();
		}

		public void close() throws IOException {
			Path outDir = new Path(PiEstimator.TMP_DIR, "out");
			Path outFile = new Path(outDir, "reduce-out");
			FileSystem fileSys = FileSystem.get(this.conf);
			SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
					this.conf, outFile, LongWritable.class, LongWritable.class,
					SequenceFile.CompressionType.NONE);

			writer.append(new LongWritable(this.numInside), new LongWritable(
					this.numOutside));
			writer.close();
		}
	}

	public static class PiMapper extends MapReduceBase implements
			Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {
		public void map(LongWritable offset, LongWritable size,
				OutputCollector<BooleanWritable, LongWritable> out,
				Reporter reporter) throws IOException {
			PiEstimator.HaltonSequence haltonsequence = new PiEstimator.HaltonSequence(
					offset.get());
			long numInside = 0L;
			long numOutside = 0L;

			for (long i = 0L; i < size.get();) {
				double[] point = haltonsequence.nextPoint();

				double x = point[0] - 0.5D;
				double y = point[1] - 0.5D;
				if (x * x + y * y > 0.25D)
					numOutside += 1L;
				else {
					numInside += 1L;
				}

				i += 1L;
				if (i % 1000L == 0L) {
					reporter.setStatus("Generated " + i + " samples.");
				}

			}

			out.collect(new BooleanWritable(true), new LongWritable(numInside));
			out.collect(new BooleanWritable(false),
					new LongWritable(numOutside));
		}
	}

	private static class HaltonSequence {
		static final int[] P = { 2, 3 };

		static final int[] K = { 63, 40 };
		private long index;
		private double[] x;
		private double[][] q;
		private int[][] d;

		HaltonSequence(long startindex) {
			this.index = startindex;
			this.x = new double[K.length];
			this.q = new double[K.length][];
			this.d = new int[K.length][];
			for (int i = 0; i < K.length; i++) {
				this.q[i] = new double[K[i]];
				this.d[i] = new int[K[i]];
			}

			for (int i = 0; i < K.length; i++) {
				long k = this.index;
				this.x[i] = 0.0D;

				for (int j = 0; j < K[i]; j++) {
					this.q[i][j] = ((j == 0 ? 1.0D : this.q[i][(j - 1)]) / P[i]);
					this.d[i][j] = ((int) (k % P[i]));
					k = (k - this.d[i][j]) / P[i];
					this.x[i] += this.d[i][j] * this.q[i][j];
				}
			}
		}

		double[] nextPoint() {
			this.index += 1L;
			for (int i = 0; i < K.length; i++) {
				for (int j = 0; j < K[i]; j++) {
					this.d[i][j] += 1;
					this.x[i] += this.q[i][j];
					if (this.d[i][j] < P[i]) {
						break;
					}
					this.d[i][j] = 0;
					this.x[i] -= (j == 0 ? 1.0D : this.q[i][(j - 1)]);
				}
			}
			return this.x;
		}
	}
}

