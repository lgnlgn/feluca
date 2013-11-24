package org.shanbo.feluca.node.job;


import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.common.FelucaJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * job for test
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

		public Nap(){
			this.jobName = "a nap";
			loop= 5;
		}
		
		
		@Override
		public void init(Properties prop) {
			
		}

		@Override
		protected String getAllLog() {
			return StringUtils.join(this.logCollector.iterator(), "");
		}


		public void stopJob() {
			state = JobState.INTERRUPTED;
		}

		public void startJob(){
			init(properties);
			state = JobState.RUNNING;
			sleeper = new Thread(new Runnable() {
				public void run() {
					for(int i = 0 ; i < loop ; i++){
						if (state == JobState.INTERRUPTED){
							log.debug(" interrupted!!!???  ");
							appendMessage(" interrupted!!!???  " );
							break;
						}
						try {
							appendMessage("let me sleep(" + i + ")" + DEFUALT_SLEEP_MS);
							Thread.sleep(DEFUALT_SLEEP_MS);
							log.debug("let me sleep(" + i + ")" + DEFUALT_SLEEP_MS);
						} catch (InterruptedException e) {
						}
					}
					state = JobState.FINISHED;
				}
			});
			sleeper.setDaemon(true);
			sleeper.start();
		}
		
	}

	public StoppableSleepJob(){
		this.jobName = "sleepjob";
	}
	
	/**
	 * use pipe log 
	 */
	protected String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
		
	}

	public void init(Properties prop) {
		super.init(prop);
		Nap n = new Nap(); //a sequancial naps, without modify it's default parameters 
		n.setLogPipe(logPipe);
		this.addSubJobs(n);
		
	}

	
	
}
