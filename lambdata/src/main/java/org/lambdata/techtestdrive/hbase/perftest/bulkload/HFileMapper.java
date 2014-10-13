package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.lambdata.techtestdrive.hbase.perftest.bulkload.BulkLoadConfig.SchemaPattern;

public class HFileMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		BulkLoadConfig bulkLoadConfig = BulkLoadConfig.fromJsonString(context
				.getConfiguration().get(BulkLoadConfig.HADOOP_CONF_KEY));
		SchemaPattern schemaPattern = bulkLoadConfig.schemaPatternAsEnum();
		int columnCount = bulkLoadConfig.getVarrayColCount();

		String line = value.toString();
		String[] items = line.split("\t", -1);
		long start = Long.valueOf(items[0]);
		long count = Long.valueOf(items[1]);
		long end = start + count;
		for (long i = start; i < end; i++) {
			byte[] rowkey = generateRowKey(i, bulkLoadConfig.getRegionCount());

			ImmutableBytesWritable rowkeyWritable = new ImmutableBytesWritable(
					rowkey);
			long seqNo = i;
			writeId(rowkey, rowkeyWritable, seqNo, context);
			writeFilterColumn(rowkey, rowkeyWritable, seqNo, context);
			if (schemaPattern.equals(SchemaPattern.STANDARD)) {
				writeFixedValueCols(3, 8, rowkey, rowkeyWritable, seqNo, 2,
						context);
			} else if (schemaPattern.equals(SchemaPattern.WIDE)) {
				writeFixedValueCols(3, 7, rowkey, rowkeyWritable, seqNo, 2,
						context);
				writeVarrayCols(columnCount, rowkey, rowkeyWritable, seqNo,
						context);
			} else if (schemaPattern.equals(SchemaPattern.SERIALIZED)) {
				writeFixedValueCols(3, 7, rowkey, rowkeyWritable, seqNo, 2,
						context);
				writeVarrayCol(rowkey, rowkeyWritable, 4, context);
			}
		}
	}

	private byte[] generateRowKey(long seqNo, int regionCount) {
		byte[] rowkey = new byte[42];
		long scanRange = 500;
		long filterCol = seqNo / scanRange;
		rowkey[0] = new BigInteger(Md5Utils.md5Hash(seqNo)).mod(
				new BigInteger(Bytes.toBytes(regionCount))).byteValue();
		rowkey[1] = '#';
		System.arraycopy(Bytes.padHead(Bytes.toBytes(filterCol), 20), 0,
				rowkey, 2, 20);
		System.arraycopy(Bytes.padHead(Bytes.toBytes(seqNo), 12), 0, rowkey,
				22, 20);
		return rowkey;
	}

	private void writeId(byte[] rowkey, ImmutableBytesWritable rowkeyWritable,
			long seqNo, Context context) {
		String family = "cf";
		String qualifier = "id";
		long columnValue = seqNo;
		KeyValue kv = new KeyValue(rowkey, Bytes.toBytes(family),
				Bytes.toBytes(qualifier), System.currentTimeMillis(),
				Bytes.toBytes(String.valueOf(columnValue)));
		try {
			context.write(rowkeyWritable, kv);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void writeFilterColumn(byte[] rowkey,
			ImmutableBytesWritable rowkeyWritable, long seqNo, Context context) {
		KeyValue kv = Cell.newCell(rowkey, "FilterCol",
				String.valueOf(seqNo / 500));
		try {
			context.write(rowkeyWritable, kv);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeFixedValueCols(int colStart, int colCount, byte[] rowkey,
			ImmutableBytesWritable rowkeyWritable, long seqNo, int howManyTen,
			Context context) {
		int colEnd = colStart + colCount;
		for (int i = colStart; i < colEnd; i++) {
			KeyValue kv = Cell.newFixedValueCell(rowkey, "Col" + i, howManyTen);

			try {
				context.write(rowkeyWritable, kv);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void writeVarrayCols(int colCount, byte[] rowkey,
			ImmutableBytesWritable rowkeyWritable, long seqNo, Context context) {
		KeyValue kv = null;

		try {
			// VarrayColCount
			kv = Cell.newCell(rowkey, "VarrayColCount", colCount);
			context.write(rowkeyWritable, kv);

			// VarrayId_X and VarrayColn_X
			writeArray("VarrayId_", colCount, rowkey, rowkeyWritable, seqNo,
					context);
			writeArray("VarrayCol1_", colCount, rowkey, rowkeyWritable, seqNo,
					context);
			writeArray("VarrayCol2_", colCount, rowkey, rowkeyWritable, seqNo,
					context);
			writeArray("VarrayCol3_", colCount, rowkey, rowkeyWritable, seqNo,
					context);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeArray(String colNamePrefix, int colCount, byte[] rowkey,
			ImmutableBytesWritable rowkeywritable, long seqNo, Context context) {
		KeyValue kv = null;
		try {
			for (int i = 0; i < colCount; i++) {
				kv = Cell.newFixedValueCell(rowkey, colNamePrefix + i, 2);
				context.write(rowkeywritable, kv);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeVarrayCol(byte[] rowkey,
			ImmutableBytesWritable rowkeyWritable, int howManyTen,
			Context context) {
		KeyValue kv = null;

		try {
			// VarrayCol
			kv = Cell.newFixedValueCell(rowkey, "VarrayCol", howManyTen);
			context.write(rowkeyWritable, kv);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		byte[] a = new HFileMapper().generateRowKey(1, 4);
		System.out.println(a.length);
	}

}
