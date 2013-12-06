package org.shanbo.feluca.node.leader.job;


import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.node.FelucaJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * job for test module
 * @author lgn
 *
 */
public class StoppableSleepJob extends FelucaJob{
	static Logger log = LoggerFactory.getLogger(StoppableSleepJob.class);
	
	static class Nap extends FelucaJob{
		static Logger log = LoggerFactory.getLogger(Nap.class);
		final static int DEFUALT_SLEEP_MS = 4000;
		int loop = 10;
		Thread sleeper;

		public Nap(JSONObject prop){
			super(prop);
			this.jobName = "a nap";
			loop= 6;
		}


		@Override
		protected String getAllLog() {
			return StringUtils.join(this.logCollector.iterator(), "");
		}


		public void stopJob() {
			state = JobState.INTERRUPTED;
		}

		public void startJob(){
			state = JobState.RUNNING;
			sleeper = new Thread(new Runnable() {
				public void run() {
					for(int i = 0 ; i < loop ; i++){
						if (state == JobState.INTERRUPTED){
							log.debug(" interrupted!!!???  ");
							logInfo(" interrupted!!!???  " );
							break;
						}
						try {
							logInfo("let me sleep(" + i + ")" + DEFUALT_SLEEP_MS);
							log.debug("let me sleep(" + i + ")" + DEFUALT_SLEEP_MS);
							Thread.sleep(DEFUALT_SLEEP_MS);
						} catch (InterruptedException e) {
						}
					}
					if (state != JobState.INTERRUPTED)
						state = JobState.FINISHED;
				}
			});
			sleeper.setDaemon(true);
			sleeper.start();
		}
		
	}

	public StoppableSleepJob(JSONObject prop){
		super(prop);
		this.jobName = "sleepjob";
		Nap n = new Nap(null); //a sequancial naps, without modify it's default parameters 
		n.setLogPipe(logPipe);
		this.addSubJobs(n);
	}
	
	/**
	 * use pipe log 
	 */
	protected String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
		
	}

	
	
}
