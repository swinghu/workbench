package org.lambdata.techtestdrive.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagGroup implements Writable {

	public static final Text BRAND = new Text("brand");
	public static final Text INTEREST = new Text("interest");

	private Text name = new Text();
	private MapWritable tags = new MapWritable();;

	public TagGroup() {

	}

	public TagGroup(Text name) {
		this.name = name;
	}

	public void addTag(Tag tag) {
		tags.put(tag.getName(), tag);
	}

	public Tag getTag(Text name) {
		return (Tag) tags.get(name);
	}

	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		tags.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		name.write(out);
		tags.write(out);
	}

}