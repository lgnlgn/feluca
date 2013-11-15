package org.shanbo.feluca.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;


/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class FelucaJob {
	
	protected long startTime ;
	protected long finishTime;
	protected final List<String> logCollector;
	protected final Properties properties;
	protected volatile JobState state;
	protected int ttl = -1;	
	
	public enum JobState{
		PENDING,
		RUNNING,
		STOPPING,
		FINISHED,
		INTERRUPTED
	}
	
	public static interface SubJob{
		/**
		 * this method is unstoppable, 
		 * in order to interrupt your job, you must split a job into many subjobs ;
		 * return execution status  
		 */
		public boolean run();
		
		public String getSubJobName();
	}
	
	
	public FelucaJob(){
		this.properties = new Properties();
		this.logCollector  = new ArrayList<String>();
		this.startTime = System.currentTimeMillis();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
	}
	
	public void setJobConfig(Properties prop){
		
		this.startTime = System.currentTimeMillis();
		if (prop != null){
			this.properties.putAll(prop);
			String expTime = prop.getProperty("job.ttl");
			if(StringUtils.isNumeric(expTime)){
				Integer t = Integer.parseInt(expTime);
				this.ttl = t;
			}
					
		}
	}
	
	
	/**
	 * unit: ms, default = -1, check by {@link JobManager}
	 * @return
	 */
	public final int getTimeToLive(){
		return ttl;
	}
	
	public final String getJobName(){
		return  this.getClass().getSimpleName();
	}
	
	public final long getJobDuration(){
		if (state == JobState.RUNNING){
			return System.currentTimeMillis() - startTime;
		}else{
			return finishTime - startTime;
		}
	}
	
	protected abstract String getExecutionLog();
	
	public void appendMessage(String content){
		if (content.endsWith("\n"))
			logCollector.add(content);
		else {
			logCollector.add(content + "\n");
		}
	}
	
	public String getJobInfo(){
		JSONObject json = new JSONObject();
		json.put("jobName", this.getJobName());
		json.put("jobCreate", startTime);
		json.put("jobDuration", getJobDuration());
		json.put("jobState", this.getJobState().toString());
		json.put("jobLog", this.getExecutionLog());
		return json.toString();
	}
	
	/**
	 * invoke by {@link JobManager}
	 * you should watch a stop signal for potential interruption. 
	 */
	public final void start(){
		this.state = JobState.RUNNING;
		Iterator<SubJob> it = splitJobToSub();
		while(it.hasNext()){
			if (this.state == JobState.RUNNING){
				SubJob subJob = it.next();
				long tStart = System.currentTimeMillis();
				boolean runSuccess = subJob.run();
				long tEnd = System.currentTimeMillis();
				if (!runSuccess){
					appendMessage("SubJob " + subJob.getSubJobName() + " failed~~~\tcost(ms):" + (tEnd - tStart));
				}else{
					appendMessage("SubJob " + subJob.getSubJobName() + " finish~~~\tcost(ms):" + (tEnd - tStart));
				}
			}else{
				// without change state
				return;
			}
		}
		this.state = JobState.FINISHED;
	}
	
	protected abstract Iterator<SubJob> splitJobToSub();
	
	/**
	 * maybe invoked by {@link JobManager}
	 * Send stop signal & distributed request for stopping, is 
	 */
	public final void stop(){
		this.state = JobState.STOPPING;
		doStopJob();
		this.state = JobState.INTERRUPTED;
	}
	
	
	protected abstract void doStopJob();
	
	public synchronized void setJobState(JobState state){
		this.state = state;
	}
	
	public synchronized JobState getJobState(){
		return this.state;
	}
	
}
