package org.lambdata.techtestdrive.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UserProfile implements Writable {

	private Text userId = new Text();

	private MapWritable tagGroups = new MapWritable();

	public UserProfile() {
		tagGroups.put(TagGroup.BRAND, new TagGroup(TagGroup.BRAND));
		tagGroups.put(TagGroup.INTEREST, new TagGroup(TagGroup.INTEREST));
	}

	public UserProfile(Text userId) {
		this();
		this.userId = userId;
	}

	public void addTag(Text groupName, Tag tag) {
		((TagGroup) tagGroups.get(groupName)).addTag(tag);
	}

	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		tagGroups.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		userId.write(out);
		tagGroups.write(out);
	}

}
