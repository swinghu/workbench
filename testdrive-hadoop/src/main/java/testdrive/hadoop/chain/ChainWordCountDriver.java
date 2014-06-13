package testdrive.hadoop.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainWordCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "ChainWordCount");
		job.setJarByClass(this.getClass());

		FileInputFormat.setInputPaths(job, "/testdrive/hadoop/input");
		FileOutputFormat.setOutputPath(job,
				new Path("/testdrive/hadoop/output"));

		Configuration conf = getConf();
		ChainMapper.addMapper(job, TokenizerMapper.class, LongWritable.class,
				Text.class, Text.class, IntWritable.class, conf);
		ChainMapper.addMapper(job, UpperCaserMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, conf);
		ChainReducer.setReducer(job, WordCountReducer.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, conf);
		ChainReducer.addMapper(job, LastMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, conf);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new ChainWordCountDriver(), args);
		System.exit(res);
	}
}
