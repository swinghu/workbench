package testdrive.hadoop.joins.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * we don’t want to keep track of the keys as they come into the reduce()
 * method. We want all the values grouped together for us.
 * 
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {

	public TaggedJoiningGroupingComparator() {
		super(TaggedKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TaggedKey taggedKey1 = (TaggedKey) a;
		TaggedKey taggedKey2 = (TaggedKey) b;
		return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
	}
}
