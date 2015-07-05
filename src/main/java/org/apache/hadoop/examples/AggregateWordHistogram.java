package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorBaseDescriptor;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorJob;

public class AggregateWordHistogram {
	public static void main(String[] args) throws IOException {
		JobConf conf = ValueAggregatorJob.createValueAggregatorJob(args,
				new Class[] { AggregateWordHistogramPlugin.class });

		JobClient.runJob(conf);
	}

	public static class AggregateWordHistogramPlugin extends
			ValueAggregatorBaseDescriptor {
		public ArrayList<Map.Entry<Text, Text>> generateKeyValPairs(Object key,
				Object val) {
			String[] words = val.toString().split(" |\t");
			ArrayList retv = new ArrayList();
			for (int i = 0; i < words.length; i++) {
				Text valCount = new Text(words[i] + "\t" + "1");
				Map.Entry en = generateEntry("ValueHistogram",
						"WORD_HISTOGRAM", valCount);

				retv.add(en);
			}
			return retv;
		}
	}
}
