package testdrive.mahout.fpm;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;

public class PfpUtils {
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
		File hadoopConfDir = PfpUtils.findHadoopConfDir();
		PfpUtils.addResource(hadoopConfDir, "core-site.xml", conf);
		PfpUtils.addResource(hadoopConfDir, "hdfs-site.xml", conf);
		PfpUtils.addResource(hadoopConfDir, "mapred-site.xml", conf);
		PfpUtils.addResource(hadoopConfDir, "yarn-site.xml", conf);
		return conf;
	}
}
