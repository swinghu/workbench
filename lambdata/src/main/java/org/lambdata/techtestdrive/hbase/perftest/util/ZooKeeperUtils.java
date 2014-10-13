package org.lambdata.techtestdrive.hbase.perftest.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.util.Bytes;

public class ZooKeeperUtils {

	public static CuratorFramework client;

	public static final String SEQUENCE_NO_PATH = "/sequences";

	public static void init() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		String zookeeperConnectionString = "localhost:2181";
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
				retryPolicy);
		client.start();
		try {
			client.create().forPath(SEQUENCE_NO_PATH, Bytes.toBytes(0L));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static long nextSequenceNo() {
		long currentSequenceNo = -1;
		InterProcessMutex lock = new InterProcessMutex(client, SEQUENCE_NO_PATH);
		try {
			lock.acquire();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			// do some work inside of the critical section here
			currentSequenceNo = Bytes.toLong(client.getData().forPath(
					SEQUENCE_NO_PATH));
			client.setData().forPath(SEQUENCE_NO_PATH,
					Bytes.toBytes((currentSequenceNo + 1)));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				lock.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return currentSequenceNo;
	}

	public static void cleanup() {
		try {
			client.delete().forPath(SEQUENCE_NO_PATH);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		init();
		System.out.println(nextSequenceNo());
		System.out.println(nextSequenceNo());
		System.out.println(nextSequenceNo());
		cleanup();
	}
}
