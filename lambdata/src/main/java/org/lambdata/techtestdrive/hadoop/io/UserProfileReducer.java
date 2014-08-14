package org.lambdata.techtestdrive.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserProfileReducer extends
		Reducer<Text, UserProfile, Text, UserProfile> {

	public void reduce(Text key, Iterable<UserProfile> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, values.iterator().next());
	}

}
