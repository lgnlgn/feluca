package org.shanbo.feluca.node.worker;

import org.shanbo.feluca.common.LogStorage;
import org.shanbo.feluca.node.FelucaJob;
import org.shanbo.feluca.node.FelucaJob.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerJobManager {
	
	static Logger log = LoggerFactory.getLogger(WorkerJobManager.class);
	
	private volatile FelucaJob running;
	
	private Thread managerThread; //
	
	public void submitTask(Object task){
		
	}
	
	public synchronized boolean isJobSlotFree(){
		if (running == null){
			return true;
		}else{
			JobState s = running.getJobState();
			log.debug("checking2 : " + s);
			if (s == JobState.FINISHED || s == JobState.INTERRUPTED){
				LogStorage.get().storeJobLogs(this.running.jobSnapshot());
				running = null;
				return true;
			}else{
				return false;
			}
		}
	}
	
}
