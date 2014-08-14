package org.lambdata.techtestdrive.hadoop.joins.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * We only consider the join key when determining which reducer the composite
 * key and data are sent to.
 * 
 */
public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text> {

	@Override
	public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
		return taggedKey.getJoinKey().hashCode() % numPartitions;
	}
}
