package org.apache.hadoop.examples.terasort;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TeraSort extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(TeraSort.class);

	public int run(String[] args) throws Exception {
		LOG.info("starting");
		JobConf job = (JobConf) getConf();
		Path inputDir = new Path(args[0]);
		inputDir = inputDir.makeQualified(inputDir.getFileSystem(job));
		Path partitionFile = new Path(inputDir, "_partition.lst");
		URI partitionUri = new URI(partitionFile.toString() + "#"
				+ "_partition.lst");

		TeraInputFormat.setInputPaths(job, new Path[] { new Path(args[0]) });
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJobName("TeraSort");
		job.setJarByClass(TeraSort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(TeraInputFormat.class);
		job.setOutputFormat(TeraOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		TeraInputFormat.writePartitionFile(job, partitionFile);
		DistributedCache.addCacheFile(partitionUri, job);
		DistributedCache.createSymlink(job);
		job.setInt("dfs.replication", 1);
		TeraOutputFormat.setFinalSync(job, true);
		JobClient.runJob(job);
		LOG.info("done");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new TeraSort(), args);
		System.exit(res);
	}

	static class TotalOrderPartitioner implements Partitioner<Text, Text> {
		private TrieNode trie;
		private Text[] splitPoints;

		private static Text[] readPartitions(FileSystem fs, Path p, JobConf job)
				throws IOException {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, job);
			List parts = new ArrayList();
			Text key = new Text();
			NullWritable value = NullWritable.get();
			while (reader.next(key, value)) {
				parts.add(key);
				key = new Text();
			}
			reader.close();
			return (Text[]) parts.toArray(new Text[parts.size()]);
		}

		private static TrieNode buildTrie(Text[] splits, int lower, int upper,
				Text prefix, int maxDepth) {
			int depth = prefix.getLength();
			if ((depth >= maxDepth) || (lower == upper)) {
				return new LeafTrieNode(depth, splits, lower, upper);
			}
			InnerTrieNode result = new InnerTrieNode(depth);
			Text trial = new Text(prefix);

			trial.append(new byte[1], 0, 1);
			int currentBound = lower;
			for (int ch = 0; ch < 255; ch++) {
				trial.getBytes()[depth] = ((byte) (ch + 1));
				lower = currentBound;
				while ((currentBound < upper)
						&& (splits[currentBound].compareTo(trial) < 0)) {
					currentBound++;
				}
				trial.getBytes()[depth] = ((byte) ch);
				result.child[ch] = buildTrie(splits, lower, currentBound,
						trial, maxDepth);
			}

			trial.getBytes()[depth] = 127;
			result.child['ÿ'] = buildTrie(splits, currentBound, upper, trial,
					maxDepth);

			return result;
		}

		public void configure(JobConf job) {
			try {
				FileSystem fs = FileSystem.getLocal(job);
				Path partFile = new Path("_partition.lst");
				this.splitPoints = readPartitions(fs, partFile, job);
				this.trie = buildTrie(this.splitPoints, 0,
						this.splitPoints.length, new Text(), 2);
			} catch (IOException ie) {
				throw new IllegalArgumentException("can't read paritions file",
						ie);
			}
		}

		public int getPartition(Text key, Text value, int numPartitions) {
			return this.trie.findPartition(key);
		}

		static class LeafTrieNode extends
				TeraSort.TotalOrderPartitioner.TrieNode {
			int lower;
			int upper;
			Text[] splitPoints;

			LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
				super();
				this.splitPoints = splitPoints;
				this.lower = lower;
				this.upper = upper;
			}

			int findPartition(Text key) {
				for (int i = this.lower; i < this.upper; i++) {
					if (this.splitPoints[i].compareTo(key) >= 0) {
						return i;
					}
				}
				return this.upper;
			}

			void print(PrintStream strm) throws IOException {
				for (int i = 0; i < 2 * getLevel(); i++) {
					strm.print(' ');
				}
				strm.print(this.lower);
				strm.print(", ");
				strm.println(this.upper);
			}
		}

		static class InnerTrieNode extends
				TeraSort.TotalOrderPartitioner.TrieNode {
			private TeraSort.TotalOrderPartitioner.TrieNode[] child = new TeraSort.TotalOrderPartitioner.TrieNode[256];

			InnerTrieNode(int level) {
				super();
			}

			int findPartition(Text key) {
				int level = getLevel();
				if (key.getLength() <= level) {
					return this.child[0].findPartition(key);
				}
				return this.child[key.getBytes()[level]].findPartition(key);
			}

			void setChild(int idx, TeraSort.TotalOrderPartitioner.TrieNode child) {
				this.child[idx] = child;
			}

			void print(PrintStream strm) throws IOException {
				for (int ch = 0; ch < 255; ch++) {
					for (int i = 0; i < 2 * getLevel(); i++) {
						strm.print(' ');
					}
					strm.print(ch);
					strm.println(" ->");
					if (this.child[ch] != null)
						this.child[ch].print(strm);
				}
			}
		}

		static abstract class TrieNode {
			private int level;

			TrieNode(int level) {
				this.level = level;
			}

			public TrieNode() {
				// TODO Auto-generated constructor stub
			}

			abstract int findPartition(Text paramText);

			abstract void print(PrintStream paramPrintStream)
					throws IOException;

			int getLevel() {
				return this.level;
			}

		}
	}
}
