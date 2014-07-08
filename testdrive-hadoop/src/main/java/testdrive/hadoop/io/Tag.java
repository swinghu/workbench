package testdrive.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Tag implements Writable {
	private Text name = new Text();
	private DoubleWritable weight = new DoubleWritable();

	public Tag() {
		
	}
	
	public Tag(String name, double weight) {
		this.name.set(name);
		this.weight.set(weight);
	}

	public Text getName() {
		return name;
	}

	public DoubleWritable getWeight() {
		return weight;
	}

	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		weight.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		name.write(out);
		weight.write(out);
	}
}
