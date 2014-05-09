package testdrive.hadoop;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver2 {

	private static final String HADOOP_CONF_DIR_KEY = "HADOOP_CONF_DIR";

	public static int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		File hadoopConfDir = findHadoopConfDir();
		addResource(hadoopConfDir, "core-site.xml", conf);
		addResource(hadoopConfDir, "hdfs-site.xml", conf);
		addResource(hadoopConfDir, "mapred-site.xml", conf);
		addResource(hadoopConfDir, "yarn-site.xml", conf);
		conf.setStrings("mapreduce.input.fileinputformat.split.maxsize", "10");
		Job job = Job.getInstance(conf, "WordCount2");
		job.setJarByClass(WordCountDriver2.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setPartitionerClass(WordLengthPartitioner.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static File findHadoopConfDir() {
		String hadoopConfPath = System.getenv(HADOOP_CONF_DIR_KEY);
		File hadoopConfDir = new File(hadoopConfPath);
		return hadoopConfDir;
	}

	private static void addResource(File hadoopConfDir, String fileName,
			Configuration conf) {
		File file = new File(hadoopConfDir, fileName);
		if (!file.exists()) {
			return;
		}
		try {
			System.out.println("added " + file);
			conf.addResource(file.toURI().toURL());
		} catch (MalformedURLException e) {
			throw new IllegalStateException(e);
		}
	}

	public static void main(String[] args) throws Exception {
		run(args);
	}

}
