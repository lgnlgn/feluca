package org.shanbo.feluca.common;

import java.util.ArrayList;
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
	protected List<String> logCollector;
	protected List<String> logPipe;
	protected final Properties properties;
	protected volatile JobState state;
	protected int ttl = -1;	

	protected String jobName;

	protected List<FelucaJob> subJobs;

	protected Thread subJobWatcher; //determine job state by subjobs


	public static enum JobState{
		PENDING,
		RUNNING,
		STOPPING,
		FINISHED,
		INTERRUPTED
	}



	public FelucaJob(){
		this.properties = new Properties();
		this.logCollector  = new ArrayList<String>();
		this.startTime = System.currentTimeMillis();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
	}

	/**
	 * specify sub job!
	 * @param prop
	 */
	public abstract void init(Properties prop);


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


	public void setLogPipe(List<String> logCollector){
		this.logPipe = logCollector;
	}


	/**
	 * unit: ms, default = -1, check by {@link JobManager}
	 * @return
	 */
	public final int getTimeToLive(){
		return ttl;
	}

	public final String getJobName(){
		return  this.jobName;
	}

	public final long getJobDuration(){
		if (state == JobState.RUNNING){
			return System.currentTimeMillis() - startTime;
		}else{
			return finishTime - startTime;
		}
	}

	protected abstract String getAllLog();

	public synchronized void appendMessage(String content){
		String line = content.endsWith("\n")?content:content+"\n";
		logCollector.add(line);
		if (this.logPipe!= null){
			logPipe.add(line);
		}
	}

	public String getJobInfo(){
		JSONObject json = new JSONObject();
		json.put("jobName", this.getJobName());
		json.put("jobCreate", startTime);
		json.put("jobDuration", getJobDuration());
		json.put("jobState", this.getJobState().toString());
		json.put("jobLog", this.getAllLog());
		return json.toString();
	}

	/**
	 * invoke by {@link JobManager}
	 * implement this method on the leaf of job-tree;
	 */
	public void startJob(){
		//start all jobs
		for(FelucaJob subJob: this.subJobs){
			subJob.startJob();
		}
		this.state = JobState.RUNNING;
		if (this.subJobs.size() > 0){
			subJobWatcher = new Thread(new Runnable() {

				public void run() {
					int action = 0;
					long tStart = System.currentTimeMillis();
					while( true){
						JobState currentState = checkAllSubJobState();
						long elapse = System.currentTimeMillis() - tStart;
						if (ttl > 0 && elapse > ttl){
							action = 1;
							break;
						}
						if (currentState == JobState.FINISHED){
							break;
						}
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
						}
					}
					if (action == 1){
						//self kill
						stopJob();
					}
				}
			}, jobName + "@watcher");
			subJobWatcher.setDaemon(true);
			subJobWatcher.start();
		}
	}

	/**
	 * 
	 * @return
	 */
	protected JobState checkAllSubJobState(){
		List<JobState> currentStates = new ArrayList<FelucaJob.JobState>(subJobs.size());
		for(FelucaJob subJob: subJobs){
			currentStates.add(subJob.getJobState());
		}
		for(JobState state : currentStates){
			if (state != JobState.FINISHED){
				return JobState.RUNNING;
			}
		}
		return JobState.FINISHED;
	}

	protected void gatherInfoFromSubJobs(){
		for(FelucaJob subJob: subJobs){
			appendMessage(String.format("%s log: %s ", subJob.getJobName(), subJob.getJobInfo()));
		}
	}


	/**
	 * invoke by {@link JobManager}
	 * implement this method on the leaf of job-tree;
	 */
	public void stopJob(){
		this.state = JobState.STOPPING;
		for(FelucaJob job : subJobs){
			job.stopJob();
		}
		this.state = JobState.INTERRUPTED;
	}

	public synchronized void setJobState(JobState state){
		this.state = state;
	}

	public synchronized JobState getJobState(){
		return this.state;
	}

}
