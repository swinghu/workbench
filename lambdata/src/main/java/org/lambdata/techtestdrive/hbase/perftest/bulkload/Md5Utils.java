package org.lambdata.techtestdrive.hbase.perftest.bulkload;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

public class Md5Utils {
	public static final int MD5_LENGTH = 16; // bytes

	public static byte[] md5Hash(String s) {
		MessageDigest d;
		try {
			d = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm not available!", e);
		}
		return d.digest(Bytes.toBytes(s));
	}

	public static byte[] md5Hash(long l) {
		MessageDigest d;
		try {
			d = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm not available!", e);
		}
		return d.digest(Bytes.toBytes(l));
	}

	public static void main(String[] args) {
		// String text = "asdfasdf";
		// System.out.println(md5Hash(text).length);
		// System.out.println(MD5Hash.digest(text).getDigest().length);
		System.out.println(new BigInteger(Bytes.padHead(Bytes.toBytes(2), 20))
				.intValue());
	}
}