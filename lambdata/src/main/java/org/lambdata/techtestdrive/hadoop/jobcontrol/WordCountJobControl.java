package org.lambdata.techtestdrive.hadoop.jobcontrol;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJobControl extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];

		String tmp = "/testdrive/hadoop/" + getClass().getSimpleName() + "-tmp";

		try {
			JobControl control = new JobControl("Top Words Job Control");
			ControlledJob step1 = new ControlledJob(Jobs.createCountJob(
					getConf(), input, tmp), null);
			control.addJob(step1);
			ControlledJob step2 = new ControlledJob(Jobs.createSortJob(
					getConf(), tmp, output), null);
			control.addJob(step2);
			step2.addDependingJob(step1);
			Thread pipelineThread = new Thread(control, "Pipeline-Thread");
			pipelineThread.setDaemon(true);
			pipelineThread.start();

			while (!control.allFinished()) {
				Thread.sleep(500);
			}

			if (control.getFailedJobList().size() > 0) {
				System.out.println("Job failed");
				for (ControlledJob job : control.getFailedJobList()) {
					System.out.println(job.getJobName() + " failed");
				}
			} else {
				System.out.println("Success!! Workflow completed ["
						+ control.getSuccessfulJobList().size() + "] jobs");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new WordCountJobControl(), args);
		System.exit(exitcode);
	}

}
