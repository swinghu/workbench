package testdrive.mahout.fpm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;

public class PfpRunner {

	public static void main(String[] args) {
		Configuration conf = PfpUtils.createHadoopConfiguration();
		Parameters params = new Parameters();
		params.set("minSupport", "2");
		params.set("maxHeapSize", "10");
		params.set("input", args[0]);
		params.set("output", args[1]);
		try {
			PFPGrowth.runPFPGrowth(params, conf);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
