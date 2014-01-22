package org.shanbo.feluca.node;

import java.lang.reflect.Constructor;
import java.util.List;

import org.shanbo.feluca.common.LogStorage;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.node.job.FelucaJob.JobState;
import org.shanbo.feluca.node.JobConductor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


/**
 * control how many jobs are allow to run
 * @author lgn
 *
 */
public class JobManager{

	public final static String JOB_NOT_FOUND = "null";
	
	static Logger log = LoggerFactory.getLogger(JobManager.class);

	private volatile FelucaJob running ; //allow only 1 job 

	private LogStorage logStorage = LogStorage.get();

	private Thread managerThread; //regularly checked job's state

	
	public JobManager(){
		this.managerThread = new Thread(new Runnable() {
			public void run(){
				while(true){
					if (isJobSlotFree()){
					}else{
					}
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						log.error("managerThread interrupted", e);
					}
				}
			}
		}, "managerThread");
		this.managerThread.setDaemon(true);
		this.managerThread.start();
	}

	/**
	 * 
	 * @param job
	 */
	private void asyncStartJob(final FelucaJob job){
		this.running = job;
		this.running.startJob();
	}

	/**
	 * 
	 * @return true if we can submit a new job; else false;
	 */
	public synchronized boolean isJobSlotFree(){
		if (running == null){
			return true;
		}else{
			JobState s = running.getJobState();
			logStorage.storeJobLogs(this.running.jobSnapshot());
			log.debug("checking by manager : " + s);
			if (s == JobState.FINISHED || s == JobState.INTERRUPTED || s == JobState.FAILED){	
				running = null;
				return true;
			}else{
				return false;
			}
		}
	}

	public JSONArray getLastJobState(){
		return getLatestJobStates(1);
	}

	public JSONArray getAllJobStates() {
		return getLatestJobStates(Integer.MAX_VALUE);
	}

	public JSONArray getLatestJobStates(int size) {
		return logStorage.getLastJobInfos(size);
	}

	public JSONObject searchJobInfo(String jobName){
		return this.logStorage.searchJobInfo(jobName);
	}
	

	public String getCurrentJobState(){
		if (!isJobSlotFree()){
			return running.toString();
		}else{ //free
			return JOB_NOT_FOUND;
		}
	}

	/**
	 * 
	 * @param jobClz
	 * @param conf
	 * @return
	 * @throws Exception
	 */
	public synchronized String asynRunJob(FelucaJob job) throws Exception{

		if (isJobSlotFree()){
			this.asyncStartJob(job);
			return job.getJobName();
		}else{
			return null;
		}
	}

	public synchronized String killJob(String jobName){
		if (this.running == null || isJobSlotFree()){
			return "no job running right now";
		}else{
			if (jobName.equals(this.running.getJobName())){
				running.stopJob();
				return "job interrupted, waiting for stop";
			}
			else
				return "input jobName not equals to the running's; job stay running";
		}
	}

}

