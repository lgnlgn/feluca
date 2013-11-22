package org.shanbo.feluca.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class FelucaJob {
	
	static Logger log = LoggerFactory.getLogger(FelucaJob.class);
	
	protected long startTime ;
	protected long finishTime;
	protected List<String> logCollector; //each job has it's own one
	protected List<String> logPipe; //you may need to share it with sub jobs 
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
		this.logCollector  = new ArrayList<String>(); //each job has it's own one
		this.logPipe = new ArrayList<String>(); //you may need to share it with sub jobs
		this.startTime = System.currentTimeMillis();
		this.finishTime = startTime;
		this.state = JobState.PENDING;
	}

	/**
	 * specify sub job!
	 * @param prop
	 */
	public void init(Properties prop){
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
	 * override this method on the leaf of job-tree;
	 * remember to start your job in background; i.e. use a thread and the jobstate
	 */
	public void startJob(){
		//start all jobs
		log.debug("subjobs:" + this.subJobs.size());
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
						if (action == 0 && ttl > 0 && elapse > ttl){
							stopJob();
							action = 1;
							log.debug("too long, kill job !");
							//then wait for JobState.FINISHED
						}
						//check stop action
						if (currentState == JobState.FINISHED){
							finishTime = System.currentTimeMillis();
							log.debug("sub jobs finished");
							break;
						}
						try {
							Thread.sleep(1000);
							log.debug("checking~~~~" + subJobs.get(0).getJobState());
							System.out.println("checking~~~~" + subJobs.get(0).getJobState());
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




	public synchronized void setJobState(JobState state){
		this.state = state;
	}

	public synchronized JobState getJobState(){
		return this.state;
	}

}
