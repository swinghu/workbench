package testdrive.hadoop.io;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class ResultReader {

	public static final String HADOOP_CONF_DIR_KEY = "HADOOP_CONF_DIR";

	public static File findHadoopConfDir() {
		String hadoopConfPath = System.getenv(HADOOP_CONF_DIR_KEY);
		File hadoopConfDir = new File(hadoopConfPath);
		return hadoopConfDir;
	}

	public static void addResource(File hadoopConfDir, String fileName,
			Configuration conf) {
		File file = new File(hadoopConfDir, fileName);
		if (!file.exists()) {
			return;
		}
		try {
			conf.addResource(file.toURI().toURL());
		} catch (MalformedURLException e) {
			throw new IllegalStateException(e);
		}
	}

	public static Configuration createHadoopConfiguration() {
		Configuration conf = new Configuration();
		File hadoopConfDir = findHadoopConfDir();
		addResource(hadoopConfDir, "core-site.xml", conf);
		return conf;
	}

	public static void main(String args[]) throws Exception {
		Configuration hadoopConfig = createHadoopConfiguration();
		FileSystem fs = FileSystem.get(hadoopConfig);
		String outputFile = "/testdrive/crunch/output/part-r-00000";
		Reader outputReader = new SequenceFile.Reader(hadoopConfig,
				SequenceFile.Reader.file(new Path(outputFile)));
		Text key = new Text();
		UserProfile value = new UserProfile();
		while (outputReader.next(key, value)) {
			System.out.println(key + ": " + value);
		}
		outputReader.close();
	}
}
