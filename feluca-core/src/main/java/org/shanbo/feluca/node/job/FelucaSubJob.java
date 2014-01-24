package org.shanbo.feluca.node.job;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.node.job.FelucaJob.JobMessage;
import org.shanbo.feluca.node.job.FelucaJob.JobState;
import org.shanbo.feluca.node.task.TaskExecutor;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * task is a special kind of job, i.e. , leaf of the leader's job-tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaSubJob{

	final static int CHECK_TASK_INTERVAL_MS = 100;
	public final static String DISTRIBUTE_ADDRESS_KEY = "address";
	
	protected TaskExecutor taskExecutor;
	protected volatile JobState state ;
	protected Logger log ;
	protected JSONObject properties = new JSONObject();
	protected FelucaJob parentJob;
	protected boolean canSubJobGo = false;

	/**
	 * input conf format -> {task:xxxx , type:xxxx, param:{xxxx}}
	 * @param prop
	 */
	public FelucaSubJob(JSONObject prop) {
		log = LoggerFactory.getLogger(this.getClass());
		this.properties.putAll(prop);
		init(); //init taskExecutor
	}
		
	
	public void setParent(FelucaJob parent){
		this.parentJob = parent;
	}
	
	public JobState getJobState(){
		return state;
	}
	
	public synchronized void logInfo(String content){
		parentJob.logInfo(content);
	}
	
	public synchronized void logError(String content, Throwable e){
		parentJob.logError(content,e);
	}
	
	
	/**
	 * through reflection 
	 */
	abstract protected void init();

	/**
	 * you must include taskrun and supervision
	 * @return
	 */
	protected abstract  Runnable createStoppableTask();

	public void stopJob(){
		if (state == JobState.RUNNING || state == JobState.PENDING)
			state = JobState.STOPPING;
	}

	public void startJob(){
		state = JobState.RUNNING;
		ConcurrentExecutor.submit(createStoppableTask());
	}


	public static FelucaSubJob decideSubJob(JSONObject parsedConf){
		if (parsedConf.getString("type").equals("local")){
			return new LocalSubJob(parsedConf);
		}else{
			return new DistributeSubJob(parsedConf);
		}
	}

	//TODO
	public static class LocalSubJob extends FelucaSubJob{

		public LocalSubJob(JSONObject prop) {
			super(prop);
		}

		@Override
		public Runnable createStoppableTask() {
			return new Runnable() {
				public void run() {
					System.out.println("taskExecutor----------run" );

					taskExecutor.execute();
					boolean killed = false;
					while(true){
						if (killed == false && state == JobState.STOPPING){
							taskExecutor.kill();
							killed = true;
						}else{
							JobState s = taskExecutor.currentState();
							if (s == JobState.FAILED || s== JobState.INTERRUPTED || s == JobState.FINISHED){
								state = s;
								break;
							}
						}
						try {
							Thread.sleep(CHECK_TASK_INTERVAL_MS);
						} catch (InterruptedException e) {
							state = JobState.INTERRUPTED;
							break;
						}
					}
				}
			};
		}

		@Override
		protected void init() {
			String taskClass = this.properties.getString("task");
			try {
				Class<? extends TaskExecutor> clz = (Class<? extends TaskExecutor>) Class.forName(taskClass);
				Constructor<? extends TaskExecutor> constructor = clz.getConstructor(JSONObject.class);
				taskExecutor = constructor.newInstance(this.properties);
			} catch (Exception e) {
				log.error("init error");
			}
			
		}

	}
	
	
	//TODO
	public static class DistributeSubJob extends FelucaSubJob{
		final static String WORKER_JOB_PATH = "/job";
		
		String address;
		String remoteJobName ;
		int retries = 2;
		
		private void startRemoteTask() throws Exception{
			String url = address + WORKER_JOB_PATH + "?action=submit";
			try{
				this.properties.put("type", "local"); //send 'local' type job to worker
				remoteJobName = JSONObject.parseObject(HttpClientUtil.get().doPost(url, properties.toString())).getString("response");
			}catch (Exception e){
				Thread.sleep(2000);
				remoteJobName = JSONObject.parseObject(HttpClientUtil.get().doPost(url, properties.toString())).getString("response");
			}
		}


		private void killRemoteTask() throws Exception{
			try{
				HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=kill&jobName=" + remoteJobName);
			}catch (Exception e){
				Thread.sleep(2000);
				HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=kill&jobName=" + remoteJobName);
			}
		}

		private JobState getRemoteTaskStatus(){
			
			try {
				String remoteTaskResponse = HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=info&jobName=" + remoteJobName);
				String jobStateString = JSONObject.parseObject(remoteTaskResponse).getJSONObject("response").getString("jobState");
				JobState state = FelucaJob.parseStateText(jobStateString);
				if (state == null ){
					retries -= 1;
					if (retries < 0){
						return JobState.FAILED;
					}else{
						return JobState.RUNNING;
					}
				}else{
					retries = 2;
				}
				return state;
			} catch (Exception e) {
				retries -= 1;
				if (retries < 0){
					return JobState.FAILED;
				}else{
					return JobState.RUNNING;
				}
			}
		}

		public DistributeSubJob(JSONObject prop) {
			super(prop);
			this.address = "http://" + prop.getString(DISTRIBUTE_ADDRESS_KEY);
		}

		@Override
		public Runnable createStoppableTask() {
			return new Runnable() {
				public void run() {
					state = JobState.PENDING;
					System.out.println("....send job to worker:" + address );
					try {
						startRemoteTask();
					} catch (Exception e1) {
						state = JobState.FAILED;
						return;
					}
					state = JobState.RUNNING;
					boolean killed = false;
					while(true){
						if (killed == false && state == JobState.STOPPING){
							try {
								killRemoteTask();
								state = JobState.INTERRUPTED;
							} catch (Exception e) {
								state = JobState.FAILED;
								return;
							}
							killed = true;
						}else{
							JobState s = getRemoteTaskStatus();
							if (s == JobState.FAILED || s== JobState.INTERRUPTED || s == JobState.FINISHED){
								state = s;
								break;
							}
						}
						log.debug("~~~~rpc~~~~ state => "  + state);
						try {
							Thread.sleep(CHECK_TASK_INTERVAL_MS);
						} catch (InterruptedException e) {
							try {
								killRemoteTask();
								state = JobState.INTERRUPTED;
							} catch (Exception e2) {
								state = JobState.FAILED;
								return;
							}
							break;
						}
					}
				}
			};
		}


		@Override
		protected void init() {
						
		}
	}

}
