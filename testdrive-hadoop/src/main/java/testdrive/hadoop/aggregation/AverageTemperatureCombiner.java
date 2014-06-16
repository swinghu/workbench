package testdrive.hadoop.aggregation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageTemperatureCombiner extends
		Reducer<Text, TemperatureAveragingPair, Text, TemperatureAveragingPair> {
	private TemperatureAveragingPair pair = new TemperatureAveragingPair();

	@Override
	protected void reduce(Text key, Iterable<TemperatureAveragingPair> values,
			Context context) throws IOException, InterruptedException {
		int temp = 0;
		int count = 0;
		for (TemperatureAveragingPair value : values) {
			temp += value.getTemp().get();
			count += value.getCount().get();
		}
		pair.set(temp, count);
		context.write(key, pair);
	}
}
