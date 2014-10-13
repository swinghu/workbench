package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {
	public static void addRow() {
		Configuration conf = HBaseConfiguration.create();
		try {
			HTable table = new HTable(conf, "table1");
			Put put = new Put(Bytes.toBytes("00001"));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("pk"),
					Bytes.toBytes("00001"));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("col1"),
					Bytes.toBytes("aaaaa"));
			table.put(put);
			table.flushCommits();
			table.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HTable getTable(String tableName) {
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return table;
	}

	public static void main(String[] args) {
		HBaseUtils.addRow();
	}
}
