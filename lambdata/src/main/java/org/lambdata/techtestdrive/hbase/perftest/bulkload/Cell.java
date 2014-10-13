package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class Cell {

	public static String TEN_BYTE = "0123456789";

	public static KeyValue newFixedValueCell(byte[] rowkey, String columnName,
			int howManyTen) {
		String columnFamily = "cf";
		String value = "";
		for (int i = 0; i < howManyTen; i++) {
			value += TEN_BYTE;
		}
		return new KeyValue(rowkey, Bytes.toBytes(columnFamily),
				Bytes.toBytes(columnName), System.currentTimeMillis(),
				Bytes.toBytes(value));
	}

	public static KeyValue newCell(byte[] rowkey, String columnName, int value) {
		String columnFamily = "cf";
		return new KeyValue(rowkey, Bytes.toBytes(columnFamily),
				Bytes.toBytes(columnName), System.currentTimeMillis(),
				Bytes.toBytes(value));
	}

	public static KeyValue newCell(byte[] rowkey, String columnName,
			String value) {
		String columnFamily = "cf";
		return new KeyValue(rowkey, Bytes.toBytes(columnFamily),
				Bytes.toBytes(columnName), System.currentTimeMillis(),
				Bytes.toBytes(value));
	}
}
