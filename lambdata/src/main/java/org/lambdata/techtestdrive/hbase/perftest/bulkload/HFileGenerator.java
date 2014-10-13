package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HFileGenerator {

	public static void generateHFile(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// Configuration conf = new Configuration();
		Configuration conf = HBaseConfiguration.create();

		String[] appArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		BulkLoadConfig bulkloadConfig = BulkLoadConfig.fromYamlFile(appArgs[0]);

		conf.set(BulkLoadConfig.HADOOP_CONF_KEY, bulkloadConfig.toJson());
		HTable table1 = HBaseUtils.getTable(bulkloadConfig.getTableName());

		Job job = Job.getInstance(conf, "HFileGenerator");

		// add the HBase and Zookeeper JAR files to the Hadoop Java classpath
		TableMapReduceUtil.addDependencyJars(job);

		job.setJarByClass(HFileGenerator.class);

		job.setMapperClass(HFileMapper.class);
		job.setReducerClass(KeyValueSortReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPath(job,
				new Path(bulkloadConfig.getRangeDir()));
		FileOutputFormat.setOutputPath(job,
				new Path(bulkloadConfig.getHfileDir()));

		HFileOutputFormat2.configureIncrementalLoad(job, table1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		generateHFile(args);
	}

}
