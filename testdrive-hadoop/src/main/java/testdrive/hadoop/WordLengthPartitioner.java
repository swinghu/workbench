package testdrive.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * words with even/odd length go to different partitions (reduce tasks).
 * 
 * @author root
 * 
 */
public class WordLengthPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		return key.toString().length() % 2;
	}

}
