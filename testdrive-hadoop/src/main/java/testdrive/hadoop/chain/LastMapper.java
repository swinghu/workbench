package testdrive.hadoop.chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LastMapper extends Mapper<Text, IntWritable, Text, Text> {

	public void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Upper Case:" + key.toString());
		String[] word = key.toString().split(",");
		context.write(new Text(word[0]), new Text(word[1]));
	}
}
