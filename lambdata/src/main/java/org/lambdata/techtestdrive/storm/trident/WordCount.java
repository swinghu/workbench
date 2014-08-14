package org.lambdata.techtestdrive.storm.trident;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCount {
	public static class Split extends BaseFunction {
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String sentence = tuple.getString(0);
			for (String word : sentence.split(" ")) {
				collector.emit(new Values(word));
			}
		}
	}

	public static class Count implements CombinerAggregator<Long> {
		public Long init(TridentTuple tuple) {
			return 1L;
		}

		public Long combine(Long val1, Long val2) {
			return val1 + val2;
		}

		public Long zero() {
			return 0L;
		}
	}

	public static StormTopology buildTopology() {
		// number of tuples in each batch is 3
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("the cow jumped over the moon"), new Values(
						"the man went to the store"), new Values(
						"four score and seven years ago"), new Values(
						"how many apples can you eat"), new Values(
						"to be or not to be the person"));
		spout.setCycle(true); // repeats these tuples forever

		TridentTopology topology = new TridentTopology();
		MemoryMapState.Factory stateFactory = new MemoryMapState.Factory();
		topology.newStream("spout1", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(stateFactory, new Count(),
						new Fields("count"));

		return topology.build();
	}

	public static void main(String[] args) {
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", conf, buildTopology());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
	}
}
