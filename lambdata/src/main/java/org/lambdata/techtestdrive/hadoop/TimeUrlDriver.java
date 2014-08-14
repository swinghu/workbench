package org.lambdata.techtestdrive.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TimeUrlDriver {

	public static class TimeUrlMapper extends
			Mapper<Text, URLWritable, Text, URLWritable> {

		public void map(Text key, URLWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class TimeUrlReducer extends
			Reducer<Text, URLWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<URLWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (URLWritable value : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}

	}

	public static class TimeUrlLineRecordReader extends
			RecordReader<Text, URLWritable> {
		private KeyValueLineRecordReader lineReader;
		private Text lineKey, lineValue;

		public TimeUrlLineRecordReader(TaskAttemptContext context,
				FileSplit split) throws IOException {
			lineReader = new KeyValueLineRecordReader(
					context.getConfiguration());
		}

		public float getProgress() throws IOException {
			return lineReader.getProgress();
		}

		public void close() throws IOException {
			lineReader.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineReader.getCurrentKey();
		}

		@Override
		public URLWritable getCurrentValue() throws IOException,
				InterruptedException {
			URLWritable url = new URLWritable();
			url.set(lineReader.getCurrentValue().toString());
			return url;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			lineReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lineReader.nextKeyValue();
		}
	}

	public static class TimeUrlTextInputFormat extends
			FileInputFormat<Text, URLWritable> {
		public RecordReader<Text, URLWritable> createRecordReader(
				InputSplit input, TaskAttemptContext context)
				throws IOException {
			return new TimeUrlLineRecordReader(context, (FileSplit) input);
		}
	}

	public static class URLWritable implements Writable {
		protected URL url;

		public URLWritable() {
		}

		public URLWritable(URL url) {
			this.url = url;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(url.toString());
		}

		public void readFields(DataInput in) throws IOException {
			url = new URL(in.readUTF());
		}

		public void set(String s) throws MalformedURLException {
			url = new URL(s);
		}
	}

	private static final String HADOOP_CONF_DIR_KEY = "HADOOP_CONF_DIR";

	public static int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		File hadoopConfDir = findHadoopConfDir();
		addResource(hadoopConfDir, "core-site.xml", conf);
		addResource(hadoopConfDir, "hdfs-site.xml", conf);
		addResource(hadoopConfDir, "mapred-site.xml", conf);
		addResource(hadoopConfDir, "yarn-site.xml", conf);
		Job job = Job.getInstance(conf, "TimeUrl");
		job.setJarByClass(TimeUrlDriver.class);
		job.setMapperClass(TimeUrlMapper.class);
		job.setReducerClass(TimeUrlReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(URLWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TimeUrlTextInputFormat.class);
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
