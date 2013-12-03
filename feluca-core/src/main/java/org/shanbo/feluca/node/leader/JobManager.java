package org.shanbo.feluca.node.leader;

import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.common.FelucaJob.JobState;
import org.shanbo.feluca.common.LogStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;


/**
 * control how many jobs are allow to run
 * @author lgn
 *
 */
public class JobManager{

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
			log.debug("checking2 : " + s);
			if (s == JobState.FINISHED || s == JobState.INTERRUPTED){
				logStorage.storeJobLogs(this.running.jobSnapshot());
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


	public String getCurrentJobState(){
		if (!isJobSlotFree()){
			return running.toString();
		}else{ //free
			return "no job running";
		}
	}

	/**
	 * 
	 * @param jobClz
	 * @param conf
	 * @return
	 * @throws Exception
	 */
	public synchronized String asynRunJob(Class<? extends FelucaJob> jobClz, Properties conf) throws Exception{

		if (isJobSlotFree()){
			Constructor<? extends FelucaJob> constructor = jobClz.getConstructor(Properties.class);
			FelucaJob job = constructor.newInstance(conf);
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

	public String toString(){
		//TODO
		return null;
	}

}

