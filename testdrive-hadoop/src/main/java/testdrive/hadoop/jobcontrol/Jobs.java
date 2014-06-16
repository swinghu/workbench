package testdrive.hadoop.jobcontrol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import testdrive.hadoop.ReverseIntComparator;

public class Jobs {

	public static class CountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public static final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(" ");
			for (String column : columns) {
				context.write(new Text(column), one);
			}
		}
	}

	public static class CountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static class SortMapper extends
			Mapper<Text, Text, IntWritable, Text> {
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(Integer.parseInt(value.toString())),
					key);
		}
	}

	public static class SortReducer extends
			Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key);
			}
		}
	}

	public static Job createCountJob(Configuration conf, String input,
			String output) throws Exception {
		Job job = Job.getInstance(conf, "Count");
		job.setJarByClass(Jobs.class);
		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		// configure mapper and reducer
		job.setMapperClass(CountMapper.class);
		job.setCombinerClass(CountReducer.class);
		job.setReducerClass(CountReducer.class);
		// configure output
		TextOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}

	public static Job createSortJob(Configuration conf, String input,
			String output) throws Exception {
		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(Jobs.class);
		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// configure mapper and reducer
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(SortReducer.class);
		job.setSortComparatorClass(ReverseIntComparator.class);
		// configure output
		TextOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}

}
