package org.lambdata.techtestdrive.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] columns = value.toString().split(" ");
		for (String column : columns) {
			context.write(new Text(column), new IntWritable(1));
		}
	}
}
