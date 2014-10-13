package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RangeGenerator {

	public static class RangeMapper extends
			Mapper<LongWritable, Text, LongWritable, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			LongWritable startWritable = new LongWritable();
			LongWritable intervalWritable = new LongWritable();

			String line = value.toString();
			String[] items = line.split(",", -1);
			long rangeCount = Long.valueOf(items[0]);
			long rangeInterval = Long.valueOf(items[1]);
			for (int i = 0; i < rangeCount; i++) {
				long start = i * rangeInterval;
				startWritable.set(start);
				intervalWritable.set(rangeInterval);
				context.write(startWritable, intervalWritable);
			}
		}
	}

	public static void generateRanges(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		BulkLoadConfig bulkloadConfig = BulkLoadConfig.fromYamlFile(appArgs[0]);

		Job job = Job.getInstance(conf, "RangeGenerator");
		job.setJarByClass(RangeGenerator.class);

		job.setMapperClass(RangeMapper.class);
		job.setNumReduceTasks(0); // mapper only

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		FileInputFormat
				.addInputPath(job, new Path(bulkloadConfig.getInitDir()));
		FileOutputFormat.setOutputPath(job,
				new Path(bulkloadConfig.getRangeDir()));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		generateRanges(args);
	}

}
