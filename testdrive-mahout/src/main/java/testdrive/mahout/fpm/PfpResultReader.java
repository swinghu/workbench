package testdrive.mahout.fpm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

public class PfpResultReader {

	public static Map<Integer, Long> readFrequency(Configuration configuration,
			String fileName) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		Reader frequencyReader = new SequenceFile.Reader(fs,
				new Path(fileName), configuration);
		Map<Integer, Long> frequency = new HashMap<Integer, Long>();
		Text key = new Text();
		LongWritable value = new LongWritable();
		while (frequencyReader.next(key, value)) {
			frequency.put(Integer.parseInt(key.toString()), value.get());
			System.out.println(key + ": " + value);
		}
		return frequency;
	}

	public static void readFrequentPatterns(Configuration configuration,
			String fileName) throws Exception {
		FileSystem fs = FileSystem.get(configuration);

		Reader frequentPatternsReader = new SequenceFile.Reader(fs, new Path(
				fileName), configuration);
		Text key = new Text();
		TopKStringPatterns value = new TopKStringPatterns();

		while (frequentPatternsReader.next(key, value)) {
			long firstFrequencyItem = -1;
			String firstItemId = null;
			List<Pair<List<String>, Long>> patterns = value.getPatterns();
			int i = 0;
			for (Pair<List<String>, Long> pair : patterns) {
				List<String> itemList = pair.getFirst();
				Long occurrence = pair.getSecond();
				System.out.println("itemList=" + itemList + ", occurrence="
						+ occurrence);
			}
		}
		frequentPatternsReader.close();

	}

	public static void main(String args[]) throws Exception {
		System.out.println("bingo");
		String baseDir = "/actinsights/underpinning/mahout/fpm/fpm-sample";
		String frequencyFilename = baseDir + "/fList";
		String frequentPatternsFilename = baseDir + "/frequentpatterns/part-r-00000";

		Configuration configuration = PfpUtils.createHadoopConfiguration();
		Map<Integer, Long> frequency = readFrequency(configuration,
				frequencyFilename);
		readFrequentPatterns(configuration, frequentPatternsFilename);
		System.out.println("done");
	}
}
