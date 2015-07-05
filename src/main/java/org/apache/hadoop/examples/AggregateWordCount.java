package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorBaseDescriptor;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorJob;

public class AggregateWordCount {
	public static void main(String[] args) throws IOException {
		JobConf conf = ValueAggregatorJob.createValueAggregatorJob(args,
				new Class[] { WordCountPlugInClass.class });

		JobClient.runJob(conf);
	}

	public static class WordCountPlugInClass extends
			ValueAggregatorBaseDescriptor {
		public ArrayList<Map.Entry<Text, Text>> generateKeyValPairs(Object key,
				Object val) {
			String countType = "LongValueSum";
			ArrayList retv = new ArrayList();
			String line = val.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				Map.Entry e = generateEntry(countType, itr.nextToken(), ONE);
				if (e != null) {
					retv.add(e);
				}
			}
			return retv;
		}
	}
}

