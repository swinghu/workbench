package org.lambdata.techtestdrive.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserProfileMapper extends
		Mapper<LongWritable, Text, Text, UserProfile> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		UserProfile profile = new UserProfile(new Text("alice"));
		String[] keywords = value.toString().split(" ");
		for (String keyword : keywords) {
			String group = "brand";
			if (keywords.length % 2 == 1) {
				group = "interest";
			}
			profile.addTag(new Text(group), new Tag(keyword, 1));
		}
		context.write(new Text("alice"), profile);
	}
}
