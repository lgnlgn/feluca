package org.shanbo.feluca.node.leader;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.common.FelucaJob.JobState;
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

	static Logger log = LoggerFactory.getLogger(JobManager.class);


	public final static int REMAIN_STATES = 10;

	private volatile FelucaJob running ; //allow only 1 job 


	protected volatile boolean runningFlag = true; //
	protected volatile boolean stoppingFlag = false;

	private AtomicInteger jobCounts = new AtomicInteger(0);
	private LinkedList<String> lastSeveralJobInfos;


	private Thread managerThread; //regularly checked job's state

	public JobManager(){

		lastSeveralJobInfos = new LinkedList<String>();
		this.managerThread = new Thread(new Runnable() {
			public void run(){
				while(true){
					if (isJobSlotFree()){
					}else{
					}
					try {
						Thread.sleep(1000);
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
				this.lastSeveralJobInfos.add(running.getJobInfo());
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
		return getLatestJobStates(this.lastSeveralJobInfos.size());
	}

	public JSONArray getLatestJobStates(int size) {
		JSONArray ja = new JSONArray();
		size = (size > this.lastSeveralJobInfos.size()) ? this.lastSeveralJobInfos.size():size;

		for(int i = size-1; i >= 0 ; i-- ){
			String info = lastSeveralJobInfos.get(i);
			ja.add(JSONObject.parse(info));
		}

		return ja;
	}


	public String getCurrentJobState(){
		if (!isJobSlotFree()){
			return running.getJobInfo();
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
			this.jobCounts.incrementAndGet();
			return job.getJobName();
		}else{
			return null;
		}
	}

	public synchronized void killJob(){
		if (this.running == null || isJobSlotFree()){
			;
		}else{
			running.stopJob();
		}
	}

	public String toString(){
		//TODO
		return null;
	}

	public void addMessageToJob(String content) {
		running.appendMessage(content);
	}
}

