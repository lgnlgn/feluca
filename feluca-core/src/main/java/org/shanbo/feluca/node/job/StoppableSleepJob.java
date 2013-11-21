package org.shanbo.feluca.node.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.common.FelucaJob;

public class StoppableSleepJob extends FelucaJob{
	
	static class Nap extends FelucaJob{
		final static int DEFUALT_SLEEP_MS = 4000;
		int loop = 10;
		Thread sleeper;
		
		
		
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
			state = JobState.RUNNING;
			sleeper = new Thread(new Runnable() {
				
				public void run() {
					for(int i = 0 ; i < loop && state != JobState.INTERRUPTED; i++){
						try {
							appendMessage("let me sleep " + DEFUALT_SLEEP_MS);
							Thread.sleep(DEFUALT_SLEEP_MS);
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
	
	
	
	@Override
	protected String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
		
	}

	@Override
	public void init(Properties prop) {
		Nap n = new Nap();
		n.setLogPipe(logPipe);
		this.subJobs.add(n);
	}

}
