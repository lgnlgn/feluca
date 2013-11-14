package org.shanbo.feluca.common;

import java.util.Iterator;
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
	protected StringBuilder logCollector;
	protected Properties properties;
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
		public void run();
	}
	
	
	public FelucaJob(){
		this.startTime = System.currentTimeMillis();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
	}
	
	public void setJobConfig(Properties prop){
		
		this.startTime = System.currentTimeMillis();
		if (prop != null){
			this.properties = prop;
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
	public int getTimeToLive(){
		return ttl;
	}
	
	public String getJobName(){
		return  this.getClass().getSimpleName();
	}
	
	public long getJobDuration(){
		if (state == JobState.RUNNING){
			return System.currentTimeMillis() - startTime;
		}else{
			return finishTime - startTime;
		}
	}
	
	protected abstract String getExecutionLog();
	
	public void appendMessage(String content){
		if (content.endsWith("\n"))
			logCollector.append(content);
		else {
			logCollector.append(content).append("\n");
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
	public void start(){
		this.state = JobState.RUNNING;
		Iterator<SubJob> it = splitJobToSub();
		while(it.hasNext()){
			if (this.state == JobState.RUNNING){
				SubJob subJob = it.next();
				subJob.run();
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
	public void stop(){
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
