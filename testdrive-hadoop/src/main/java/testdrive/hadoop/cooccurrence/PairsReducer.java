package testdrive.hadoop.cooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairsReducer extends
		Reducer<WordPair, IntWritable, WordPair, IntWritable> {
	private IntWritable totalCount = new IntWritable();

	@Override
	protected void reduce(WordPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		totalCount.set(count);
		context.write(key, totalCount);
	}
}
