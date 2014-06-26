package org.shanbo.feluca.node.job;

import java.lang.reflect.Constructor;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.node.job.JobState;
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

	final static int CHECK_TASK_INTERVAL_MS = 200;
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
		state = JobState.PENDING;
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


	/**
	 * 
	 * @param parsedConf
	 * @return
	 */
	public static FelucaSubJob decideSubJob(JSONObject parsedConf){
		if (parsedConf.containsKey(DISTRIBUTE_ADDRESS_KEY))
			return new DistributeSubJob(parsedConf);
		else{
			return new LocalSubJob(parsedConf);
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
					if (taskExecutor == null){ //initialization failed!!!!
						state = JobState.FAILED;
						return;
					}
					System.out.println("local taskExecutor----------run (say by LocalSubJob)" );
					taskExecutor.execute(); //
					boolean killed = false;
					while(true){
						if (killed == false && state == JobState.STOPPING){
							taskExecutor.kill(); //send a signal
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
					logInfo(taskExecutor.getTaskFinalMessage());
				}

			};
		}

		@Override
		protected void init() {
			String taskClass = this.properties.getString("task");
			try {
				@SuppressWarnings("unchecked")
				Class<? extends TaskExecutor> clz = (Class<? extends TaskExecutor>) Class.forName(taskClass);
				Constructor<? extends TaskExecutor> constructor = clz.getConstructor(JSONObject.class);
				taskExecutor = constructor.newInstance(this.properties);
			} catch (Exception e) {
				System.out.println(".? error??????????");
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
				this.properties.put("type", "local"); //change 'local' type job for worker, worker uses it to start local tasks
				this.properties.getJSONObject("param").put("repo", Constants.Base.getWorkerRepository()); //change repo for workers
				remoteJobName = JSONObject.parseObject(HttpClientUtil.get().doPost(url, properties.toString())).getString("response");
			}catch (Exception e){
				Thread.sleep(2000);
				remoteJobName = JSONObject.parseObject(HttpClientUtil.get().doPost(url, properties.toString())).getString("response");
			}
		}

		private String fetchRemoteTaskMessage(){
			try{
				String remoteTaskResponse = HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=info&jobName=" + remoteJobName);
				JSONObject resp = JSONObject.parseObject(remoteTaskResponse).getJSONObject("response");
				return resp.getString("jobLog");
			}catch (Exception e){
				try{
					Thread.sleep(2000);
					String remoteTaskResponse = HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=info&jobName=" + remoteJobName);
					JSONObject resp = JSONObject.parseObject(remoteTaskResponse).getJSONObject("response");
					return resp.getString("jobLog");
				}catch( Exception e2){
					return "lost remote task messge";
				}
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
				JSONObject remoteResult = JSONObject.parseObject(HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=info&jobName=" + remoteJobName));
				if (remoteResult.containsValue("null")){ //remote task is not started yet
					retries -= 1;
					return JobState.RUNNING;
				}
				String jobStateString = remoteResult.getJSONObject("response").getString("jobState");
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
					System.out.println("DistributeSubJob ....send job to worker:" + address );
					try {
						startRemoteTask();
						logInfo("remoteTask:" + remoteJobName);
					} catch (Exception e1) {
						logError("send job to worker error! ", e1);
						state = JobState.FAILED;
						return;
					}
					state = JobState.RUNNING;
					boolean killed = false;
					while(true){
						try {
							Thread.sleep(CHECK_TASK_INTERVAL_MS);
						} catch (InterruptedException e) {
							try {
								killRemoteTask();
								state = JobState.INTERRUPTED;
							} catch (Exception e2) {
								logError("killRemoteTask error ", e);
								state = JobState.FAILED;
								return;
							}
							break;
						}
						if (killed == false && state == JobState.STOPPING){
							try {
								killRemoteTask();
								state = JobState.INTERRUPTED;
							} catch (Exception e) {
								logError("loss connection with worker ", e);
								state = JobState.FAILED;
								return;
							}
							killed = true;
						}else{
							JobState s = getRemoteTaskStatus();
							log.debug("~~~~rpc~~~~ state => "  + s);
							if (s == JobState.FAILED || s== JobState.INTERRUPTED || s == JobState.FINISHED){
								logInfo(fetchRemoteTaskMessage());
								state = s;
								break;
							}
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
