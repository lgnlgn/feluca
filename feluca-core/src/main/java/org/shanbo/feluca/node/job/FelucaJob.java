package org.shanbo.feluca.node.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.Strings;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


/**
 *  common job
 *  @Description: TODO
 *	@author shanbo.liang
 */
public class FelucaJob {

	public static Map<String, TaskExecutor> TASKS = new HashMap<String, TaskExecutor>();
	
	
	
	public static final String JOB_NAME = "jobName";
	public static final String JOB_START_TIME = "jobStart";
	public static final String JOB_DETAIL = "jobParameters";
	public static final String TIME_TO_LIVE = "job.ttl";

	protected Logger log ;

	protected TaskExecutor confParser;//only for parse 
	
	protected boolean isLegal;
	protected final long startTime ;
	protected long finishTime;
//	protected List<JobMessage> logCollector; //each job has it's own one
	protected List<JobMessage> logPipe ; //you may need to share it with sub jobs 
	protected final JSONObject properties;
	protected volatile JobState state;
	protected int ttl = -1;	
	
	protected String jobName;

	protected List<List<FelucaSubJob>> subJobs;
	protected List<List<JobState>> subJobStates;
	protected int runningJobs = 0;
	
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

	public static void addGlobalTask(TaskExecutor task){
		TASKS.put(task.getTaskName(), task);
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
//		this.logCollector  = new ArrayList<JobMessage>(); //each job has it's own one
		this.logPipe = new ArrayList<JobMessage>(); //you may need to share it with sub jobs
		this.startTime = DateUtil.getMsDateTimeFormat();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
		if (!isLegal){
			return;
		}
		if (prop != null){
			this.properties.putAll(prop);
			String expTime = prop.getString(TIME_TO_LIVE);
			if(StringUtils.isNumeric(expTime)){
				Integer t = Integer.parseInt(expTime);
				this.ttl = t;
			}
		}
		//		this.jobName = JSONUtil.getJson(properties, JOB_NAME, "felucaJob_" + startTime) + startTime;
		this.log = LoggerFactory.getLogger(this.getClass());
		this.confParser = TASKS.get(properties.get("task"));
		this.generateSubJobs();
		this.subJobStates = new ArrayList<List<JobState>>(this.subJobs.size());
		for(int i = 0 ; i < subJobs.size(); i++){
			subJobStates.add(new ArrayList<JobState>());
		}
	}

	private boolean generateSubJobs(){
		JSONArray subJobAllocation = confParser.parseConfForJob();
		if (subJobAllocation == null)
			return false;
		this.subJobs = new ArrayList<List<FelucaSubJob>>(subJobAllocation.size());
		for(int i = 0 ; i < subJobAllocation.size();i++){
			JSONArray concurrentJobAllocation = subJobAllocation.getJSONArray(i);
			List<FelucaSubJob> concurrentJobs = new ArrayList<FelucaSubJob>(concurrentJobAllocation.size());
			this.subJobs.add(concurrentJobs);
			for(int j = 0; j < concurrentJobAllocation.size(); j++){
				concurrentJobs.add(FelucaSubJob.decideSubJob(concurrentJobAllocation.getJSONObject(j)));
			}
		}
		return true;
	}

	public void setLogPipe(List<JobMessage> logCollector){
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
			return DateUtil.getMsDateTimeFormat() - startTime;
		}else{
			return finishTime - startTime;
		}
	}

	public String getAllLog(){
		return StringUtils.join(this.logPipe.iterator(), "\n");
	}

	public synchronized void logInfo(String content){
		String line = content.endsWith("\n")?content:content+"\n";
		JobMessage msg = new JobMessage(Strings.INFO, line, DateUtil.getMsDateTimeFormat());
//		logCollector.add(msg);
		if (this.logPipe!= null){
			logPipe.add(msg);
		}
	}

	public synchronized void logError(String errorHead, Throwable e){
		String line = errorHead.endsWith("\n")?errorHead:errorHead+"\n";
		JobMessage msg = new JobMessage(Strings.ERROR, line + Strings.throwableToString(e), DateUtil.getMsDateTimeFormat());
//		logCollector.add(msg);
		if (this.logPipe!= null){
			logPipe.add(msg);
		}
	}

	public boolean isLegal() {
		return isLegal;
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


	
	private void startJobs(List<FelucaSubJob> subJobs){
		for(FelucaJob subJob: subJobs){
			subJob.startJob();
		}
	}

	protected void checkSubJobs(){
		
	}
	
	
	/**
	 * invoke by {@link JobManager}
	 * override this method on the leaf of job-tree;
	 * remember to start your job in background; i.e. use a thread and the jobstate
	 */
	public void startJob(){
		//start all jobs
		log.debug("subjobs:" + this.subJobs.size());
		this.state = JobState.RUNNING;
		
		ConcurrentExecutor.submit(new Thread(new Runnable() {
			public void run() {
				startJobs(subJobs.get(runningJobs));
				int action = 0;
				long tStart = DateUtil.getMsDateTimeFormat();
				JobState currentJobState = JobState.FINISHED;
				startJobs(subJobs.get(runningJobs));
				while (runningJobs < subJobs.size()){

					List<JobState> tmp = checkAllSubJobState(subJobs.get(runningJobs));
					subJobStates.set(runningJobs, tmp);
					currentJobState = evaluateJobState(tmp);
					
				
					long elapse = DateUtil.getMsDateTimeFormat() - tStart;
					if (action == 0 && ttl > 0 && elapse > ttl){
						stopJob();
						action = 1;
						log.debug("too long, kill job !");
						//then wait for JobState.FINISHED
					}
					//check stop action
					if (currentJobState == JobState.FINISHED){
						finishTime = DateUtil.getMsDateTimeFormat();
						log.debug("sub jobs finished");
						runningJobs +=1;
						if (runningJobs >= subJobs.size()){
							state = JobState.FINISHED;
							break;
						}else{
							startJobs(subJobs.get(runningJobs));
						}
					}else if (currentJobState == JobState.INTERRUPTED){
						finishTime = DateUtil.getMsDateTimeFormat();
						log.debug("sub jobs interrupted");
						state = JobState.INTERRUPTED;
						break;
					}else if (currentJobState == JobState.FAILED){
						finishTime = DateUtil.getMsDateTimeFormat();
						log.debug("sub jobs faild");
						state = JobState.FAILED;
						break;
					}

					try {
						Thread.sleep(200);
						log.debug("checking~~~~" + currentJobState.toString());
//						System.out.println("checking~~~~" + subJobs.get(0).getJobState());
					} catch (InterruptedException e) {
					}
				}
			}
		}, jobName + "@watcher"));


	}

	/**
	 * invoke by {@link JobManager}
	 * override this method on the leaf of job-tree;
	 * you must make sure the stop action take effect in  finite duration
	 */
	public void stopJob(){
		if (subJobs == null || runningJobs > subJobs.size()){
			;
		}else{
			if (this.state != JobState.INTERRUPTED && this.state != JobState.FAILED){
				this.state = JobState.STOPPING;
				for(FelucaJob job : subJobs.get(runningJobs)){
					job.stopJob();
				}
			}
		}
	}

	public static JobState evaluateJobState(List<JobState> currentStates){
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
	protected List<JobState> checkAllSubJobState(List<FelucaSubJob> tasks){
		List<JobState> currentStates = new ArrayList<FelucaJob.JobState>(tasks.size());
		for(FelucaJob subJob: tasks){
			currentStates.add(subJob.getJobState());
		}
		return currentStates;
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
