package testdrive.javaessentials.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TypesafeConfigTest {

	public static void load1() {
		// default application.conf
		Config conf = ConfigFactory.load();
		System.out.println(conf.getInt("foo.bar"));
	}

	public static void load2() {
		Config conf = ConfigFactory.load("application2");
		System.out.println(conf.getInt("foo.bar"));
	}

	public static void load3() {
		// classpath resource
		System.setProperty("config.resource", "application3.conf");
		Config conf = ConfigFactory.load();
		System.out.println(conf.getInt("foo.bar"));
	}

	public static void load4() {
		// file system path
		System.setProperty("config.file", "src/main/config/application4.conf");
		Config conf = ConfigFactory.load();
		System.out.println(conf.getInt("foo.bar"));
	}

	public static void main(String[] args) {
		load4();
	}
}
