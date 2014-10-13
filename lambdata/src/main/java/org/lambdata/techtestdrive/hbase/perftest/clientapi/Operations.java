package org.lambdata.techtestdrive.hbase.perftest.clientapi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class Operations {
	static Configuration hbaseConfig = null;

	public static void main(String[] args) throws Exception {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.master", "192.168.230.133:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.230.133");
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);
		insert(false, 1024 * 1024 * 24);
		insert(true, 0);
		insert(true, 0);
	}

	private static void insert(boolean autoFlush, long writeBuffer)
			throws IOException {
		String tableName = "etltest";
		HBaseAdmin hAdmin = new HBaseAdmin(hbaseConfig);
		if (hAdmin.tableExists(tableName)) {
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		HTableDescriptor t = new HTableDescriptor(tableName);
		t.addFamily(new HColumnDescriptor("f1"));
		t.addFamily(new HColumnDescriptor("f2"));
		t.addFamily(new HColumnDescriptor("f3"));
		t.addFamily(new HColumnDescriptor("f4"));
		hAdmin.createTable(t);
		System.out.println("table created");

		HTable table = new HTable(hbaseConfig, tableName);
		table.setAutoFlush(autoFlush);
		if (writeBuffer != 0) {
			table.setWriteBufferSize(writeBuffer);
		}
		List<Put> lp = new ArrayList<Put>();
		long all = System.currentTimeMillis();

		System.out.println("start time = " + all);
		int count = 10000;
		byte[] buffer = new byte[128];
		Random r = new Random();
		for (int i = 1; i <= count; ++i) {
			Put p = new Put(String.format("row d", i).getBytes());
			r.nextBytes(buffer);
			p.add("f1".getBytes(), null, buffer);
			p.add("f2".getBytes(), null, buffer);
			p.add("f3".getBytes(), null, buffer);
			p.add("f4".getBytes(), null, buffer);
			lp.add(p);
			if (i % 1000 == 0) {
				table.put(lp);
				lp.clear();
			}
		}

		System.out.println("autoFlush=" + autoFlush + ",buffer=" + writeBuffer
				+ ",count=" + count);
		long end = System.currentTimeMillis();
		System.out.println("total need time = " + (end - all) * 1.0 / 1000
				+ "s");

		System.out.println("insert complete" + ",costs:"
				+ (System.currentTimeMillis() - all) * 1.0 / 1000 + "ms");
	}
}
