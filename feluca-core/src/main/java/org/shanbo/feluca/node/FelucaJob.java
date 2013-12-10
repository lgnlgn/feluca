package org.shanbo.feluca.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.ftpserver.util.DateUtils;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class FelucaJob {
	
	public static final String JOB_NAME = "jobName";
	public static final String JOB_START_TIME = "jobStart";
	public static final String JOB_DETAIL = "jobParameters";
	public static final String TIME_TO_LIVE = "job.ttl";
	
	protected Logger log ;
	
	protected long startTime ;
	protected long finishTime;
	protected List<JobMessage> logCollector; //each job has it's own one
	protected List<JobMessage> logPipe; //you may need to share it with sub jobs 
	protected final JSONObject properties;
	protected volatile JobState state;
	protected int ttl = -1;	

	protected String jobName;

	protected List<FelucaJob> subJobs;

	protected Thread subJobWatcher; //determine job state by subjobs

	static HashMap<String, JobState> jobStateMap = new HashMap<String, FelucaJob.JobState>();
	static {
		for(JobState js : JobState.values()){
			jobStateMap.put(js.toString(), js);
		}
	}

	public static enum JobState{
		PENDING,
		RUNNING,
		STOPPING,
		FINISHED,
		INTERRUPTED,
		FAILED
	}

	public static class JobMessage{
		String logType; //info warn error
		String logContent;
		long logTime;
		
		public JobMessage(String logType, String content, long ms){
			this.logContent = content;
			this.logTime = ms;
			this.logType = logType;
		}
		
	}

	public FelucaJob(JSONObject prop){
		this.properties = new JSONObject();
		this.logCollector  = new ArrayList<JobMessage>(); //each job has it's own one
		this.logPipe = new ArrayList<JobMessage>(); //you may need to share it with sub jobs
		this.startTime = DateUtil.getMsDateTimeFormat();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
		
		if (prop != null){
			this.properties.putAll(prop);
			String expTime = prop.getString(TIME_TO_LIVE);
			if(StringUtils.isNumeric(expTime)){
				Integer t = Integer.parseInt(expTime);
				this.ttl = t;
			}
		}
		this.jobName = JSONUtil.getJson(properties, JOB_NAME, "felucaJob_" + startTime);
		this.log = LoggerFactory.getLogger(this.getClass());
	}



	public void setLogPipe(List<JobMessage> logCollector){
		this.logPipe = logCollector;
	}

	public synchronized void addSubJobs(FelucaJob... subJobs){
		if (this.subJobs == null){
			this.subJobs = new ArrayList<FelucaJob>();
		}
		for(FelucaJob subJob: subJobs)
			this.subJobs.add(subJob);
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
			return DateUtil.getMsDateTimeFormat() - startTime;
		}else{
			return finishTime - startTime;
		}
	}

	protected abstract String getAllLog();

	public synchronized void logInfo(String content){
		String line = content.endsWith("\n")?content:content+"\n";
		JobMessage msg = new JobMessage(Strings.INFO, line, DateUtil.getMsDateTimeFormat());
		logCollector.add(msg);
		if (this.logPipe!= null){
			logPipe.add(msg);
		}
	}
	
	public synchronized void logError(String errorHead, Throwable e){
		String line = errorHead.endsWith("\n")?errorHead:errorHead+"\n";
		JobMessage msg = new JobMessage(Strings.ERROR, line + Strings.throwableToString(e), DateUtil.getMsDateTimeFormat());
		logCollector.add(msg);
		if (this.logPipe!= null){
			logPipe.add(msg);
		}
	}
	
	

	public String toString(){
		return this.jobSnapshot().toJSONString();
	}
	
	public JSONObject jobSnapshot(){
		JSONObject json = new JSONObject();
		json.put(JOB_NAME, this.getJobName());
		json.put(JOB_START_TIME, startTime);
		json.put("jobDuration", getJobDuration());
		json.put("jobState", this.getJobState().toString());
		json.put("jobLog", this.getAllLog());
		return json;
	}
	

	/**
	 * invoke by {@link JobManager}
	 * override this method on the leaf of job-tree;
	 * remember to start your job in background; i.e. use a thread and the jobstate
	 */
	public void startJob(){
		//start all jobs
		log.debug("subjobs:" + this.subJobs.size());
		if (this.subJobs == null){ //no sub job , early return 
			this.state = JobState.RUNNING;
			return; 
		}
		for(FelucaJob subJob: this.subJobs){
			subJob.startJob();
		}
		this.state = JobState.RUNNING;
		if (this.subJobs.size() > 0){
			subJobWatcher = new Thread(new Runnable() {
				public void run() {
					int action = 0;
					long tStart = DateUtil.getMsDateTimeFormat();
					while( true){
						JobState subjobState = checkAllSubJobState();
						long elapse = DateUtil.getMsDateTimeFormat() - tStart;
						if (action == 0 && ttl > 0 && elapse > ttl){
							stopJob();
							action = 1;
							log.debug("too long, kill job !");
							//then wait for JobState.FINISHED
						}
						//check stop action
						if (subjobState == JobState.FINISHED){
							finishTime = DateUtil.getMsDateTimeFormat();
							log.debug("sub jobs finished");
							state = JobState.FINISHED;
							break;
						}else if (subjobState == JobState.INTERRUPTED){
							finishTime = DateUtil.getMsDateTimeFormat();
							log.debug("sub jobs interrupted");
							state = JobState.INTERRUPTED;
							break;
						}else if (subjobState == JobState.FAILED){
							finishTime = DateUtil.getMsDateTimeFormat();
							log.debug("sub jobs faild");
							state = JobState.FAILED;
							break;
						}
						try {
							Thread.sleep(200);
							log.debug("checking~~~~" + subJobs.get(0).getJobState());
//							System.out.println("checking~~~~" + subJobs.get(0).getJobState());
						} catch (InterruptedException e) {
						}
					}
				}
			}, jobName + "@watcher");
			subJobWatcher.setDaemon(true);
			subJobWatcher.start();
		}
	}

	/**
	 * invoke by {@link JobManager}
	 * override this method on the leaf of job-tree;
	 * you must make sure the stop action take effect in  finite duration
	 */
	public void stopJob(){
		this.state = JobState.STOPPING;
		for(FelucaJob job : subJobs){
			job.stopJob();
		}
	}
	
	public static JobState checkAllSubJobState(List<JobState> currentStates){
		int allStates = 0;
		for(JobState state : currentStates){
			if (state == JobState.FINISHED){
				allStates |= 0x1;
			}else if (state == JobState.INTERRUPTED){
				allStates |= 0x2;
			}else if (state == JobState.FAILED){
				allStates |= 0x4;
			}else{
				allStates |= 0x8;
			}
		}
		if (allStates <= 1){
			return JobState.FINISHED;
		}else if (allStates <= 3){
			return JobState.INTERRUPTED;
		}else if (allStates <= 5){
			return JobState.FAILED;
		}else {
			return JobState.RUNNING;
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
		return checkAllSubJobState(currentStates);
	}

//	protected void gatherInfoFromSubJobs(){
//		for(FelucaJob subJob: subJobs){
//			appendMessage(String.format("%s log: %s ", subJob.getJobName(), subJob.getJobInfo()));
//		}
//	}




	public synchronized void setJobState(JobState state){
		this.state = state;
	}

	public synchronized JobState getJobState(){
		return this.state;
	}

	public static JobState parseText(String text){
		if(StringUtils.isBlank(text))
			return null;
		return jobStateMap.get(text);
	}
	
	
}
