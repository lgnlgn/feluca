package org.shanbo.feluca.node.job;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.common.FelucaJob;

public class StoppableSleepJob extends FelucaJob{
	
	class Nap implements SubJob{

		String name;
		int sleep;
		public Nap(int id, int msToSleep){
			this.name = "nap(" + id+ ")";
			this.sleep = msToSleep;
		}
		
		public boolean run() {
			try {
				Thread.sleep(sleep);
				return true;
			} catch (InterruptedException e) {
				appendMessage("sleep subjob interrupted~~~");
			}
			return false;
		}

		public String getSubJobName() {
			return name;
		}
	}
	
	final static int DEFUALT_SLEEP_MS = 4000;
	
	@Override
	protected String getExecutionLog() {
		return StringUtils.join(this.logCollector.iterator(), "");
	}

	@Override
	protected Iterator<SubJob> splitJobToSub() {
		List<SubJob> sleeps = new ArrayList<FelucaJob.SubJob>();
		for(int i = 0 ; i < 10; i+=1){
			sleeps.add(new Nap(i, DEFUALT_SLEEP_MS));
		}
		return sleeps.iterator();
	}

	@Override
	protected void doStopJob() {
		//do nothing;		
	}

}
