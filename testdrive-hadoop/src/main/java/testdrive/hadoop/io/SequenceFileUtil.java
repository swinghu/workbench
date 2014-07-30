package testdrive.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceFileUtil {

	public static void readSequenceFile(String path) {
		Configuration conf = new Configuration();
		Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf,
					SequenceFile.Reader.file(new Path(path)));
			Text key = new Text();
			Text value = new Text();
			while (reader.next(key, value)) {
				System.out.println(key + " : " + value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void writeSequenceFile(String path) {
		Configuration conf = new Configuration();
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(conf,
					SequenceFile.Writer.file(new Path(path)),
					SequenceFile.Writer.keyClass(Text.class),
					SequenceFile.Writer.valueClass(UserProfile.class));
			writer.append(new Text("001"), new Text("00001"));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {

	}
}
