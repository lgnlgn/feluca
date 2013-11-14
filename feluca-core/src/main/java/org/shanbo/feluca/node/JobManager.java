package org.shanbo.feluca.node;

import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.common.FelucaJob.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;



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
						if (running.getTimeToLive() > 0 && running.getJobDuration() > running.getTimeToLive()){
							killJob();
						}
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						log.error("managerThread interrupted", e);
					}
				}
			}
		}, "managerThread");
		this.managerThread.setDaemon(true);
		this.managerThread.start();
	}

	
	private void clearJob(){
		JobState s = running.getJobState();
		if (s == JobState.FINISHED || s == JobState.INTERRUPTED){

			lastSeveralJobInfos.addLast(running.getJobInfo());
			if (lastSeveralJobInfos.size() > REMAIN_STATES){
				lastSeveralJobInfos.pollFirst();
			}
			running = null;
			System.gc();
		}else{
			
		}
		
		
		
	}
	
	/**
	 * 
	 * @param job
	 */
	private void asyncStartJob(final FelucaJob job){
		this.running = job;
		ConcurrentExecutor.submit(new Runnable() {	
			public void run() {
				running.start();				
			}
		});
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
			if (s == JobState.FINISHED || s == JobState.INTERRUPTED){
				clearJob();
				return true;
			}else{
				return false;
			}
		}
	}
	
	public String getLastJobState(){
		return this.lastSeveralJobInfos.peekLast();
	}
	
	public JSONArray getLatestJobStates() {
		JSONArray ja = new JSONArray();
		ja.addAll(this.lastSeveralJobInfos);
		return ja;
	}
	
	public String getCurrentJobState(){
		if (!isJobSlotFree()){
			return running.getJobInfo();
		}else{ //free
			return " no job running ";
		}
	}
	
	/**
	 * 
	 * @param job
	 * @return
	 */
	public synchronized boolean asynRunJob(final FelucaJob job){
		if (isJobSlotFree()){
			this.jobCounts.incrementAndGet();
			this.asyncStartJob(job);
			return true;
		}else{
			return false;
		}
	}
	
	public synchronized void killJob(){
		if (this.running == null || isJobSlotFree()){
			;
		}else{
			ConcurrentExecutor.submit(new Runnable() {	
				public void run() {
					running.stop();				
				}
			});
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

