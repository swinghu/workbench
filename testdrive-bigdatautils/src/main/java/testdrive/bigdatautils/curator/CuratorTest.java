package testdrive.bigdatautils.curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorTest {
	public static CuratorFramework client;

	public static void startClient() {
		String zookeeperConnectionString = "localhost:2181";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.newClient(zookeeperConnectionString,
				retryPolicy);
		client.start();
	}

	public static void callingZookeeperDirectly() throws Exception {
		byte[] myData = "this is my data".getBytes();
		String path = "/my";
		client.create().forPath(path, myData);
		System.out.println(new String(client.getData().forPath(path)));
		client.delete().forPath(path);
	}

	public static void stopClient() {
		if (client != null) {
			client.close();
		}
	}

	public static void distributedLock() throws Exception {
		String lockPath = "/locks/1";
		long maxWait = 3;
		TimeUnit waitUnit = TimeUnit.SECONDS;
		InterProcessMutex lock = new InterProcessMutex(client, lockPath);
		if (lock.acquire(maxWait, waitUnit)) {
			try {
				// do some work inside of the critical section here
			} finally {
				lock.release();
			}
		}
	}

	public static void leaderElection() {
		LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
			public void takeLeadership(CuratorFramework client)
					throws Exception {
				// this callback will get called when you are the leader
				// do whatever leader work you need to and only exit
				// this method when you want to relinquish leadership
			}
		};
	}

	public static void main(String[] args) throws Exception {
		startClient();
		callingZookeeperDirectly();
		stopClient();
	}
}
