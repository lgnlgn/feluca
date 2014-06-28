package org.shanbo.feluca.node.job;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.node.job.local.LocalOneStepJob;
import org.shanbo.feluca.node.job.remote.RemoteAllOneStepJob;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableSet;

/**
 * task is a special kind of job, i.e. , leaf of the leader's job-tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaSubJob{

	final static int CHECK_TASK_INTERVAL_MS = 200;
	public final static String DISTRIBUTE_ADDRESS_KEY = "address";

	private static Map<String, SubJobAllocator> SUBJOBS = new HashMap<String, SubJobAllocator>();
	

	private static void addJob(SubJobAllocator job){
		SUBJOBS.put(job.getJobName(), job);
	}
	
	public static String getTaskClass(String abbrTaskName){
		return SUBJOBS.get(abbrTaskName).getClass().getName();
	}
	
	
	
	static{
		addJob(new LocalOneStepJob("ldelete", "filedelete"));
		addJob(new LocalOneStepJob("lsleep", "sleep"));
		addJob(new LocalOneStepJob("lruntime", "runtime"));
		addJob(new RemoteAllOneStepJob("ldelete", "filedelete"));
		addJob(new RemoteAllOneStepJob("dsleep", "sleep"));
		addJob(new RemoteAllOneStepJob("rruntime", "runtime"));

	}
	
	
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

	public static JSONArray allocateSubJobs(JSONObject udConf){
		return SUBJOBS.get(udConf.get("task")).allocateSubJobs(udConf);
	}
	
	
	public static Set<String> showJobList(){
		return ImmutableSet.copyOf(SUBJOBS.keySet());
	}
	

	/**
	 * only invoke by <b>local-type</b> SubjobAllocators 
	 * @param parsedConf
	 */
	public static void toLeaderBeforeDecide(JSONObject parsedConf){
		parsedConf.remove(DISTRIBUTE_ADDRESS_KEY);
		parsedConf.put("type", "local"); //change 'local' type job for worker, worker uses it to start local tasks
		parsedConf.getJSONObject("param").put("repo", Constants.Base.getLeaderRepository()); //change repo for workers
	}
	

	
	
	public static boolean isSubJobLocal(JSONObject udConf){
		if ("local".equals(udConf.getString("type"))){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * 
	 * @param parsedConf
	 * @return
	 */
	public static FelucaSubJob decideSubJob(JSONObject parsedConf){
		if (isSubJobLocal(parsedConf)){
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
			String taskClass = SubJobAllocator.getTask(this.properties.getString("task")).getClass().getName();
			try {
				@SuppressWarnings("unchecked")
				Class<? extends TaskExecutor> clz = (Class<? extends TaskExecutor>) Class.forName(taskClass);
				Constructor<? extends TaskExecutor> constructor = clz.getConstructor(JSONObject.class);
				taskExecutor = constructor.newInstance(this.properties);
			} catch (Exception e) {
				System.err.println("init class error??????????" + e.getMessage());
				log.error("init class error??????????", e);
			}

		}

	}


	//TODO
	public static class DistributeSubJob extends FelucaSubJob{
		final static String WORKER_JOB_PATH = "/job";

		String address;
		String remoteJobName ;
		int retries = 2;

		static void ticketToWorker(JSONObject parsedConf){
			parsedConf.remove(DISTRIBUTE_ADDRESS_KEY);
			parsedConf.put("type", "local"); //change 'local' type job for worker, worker uses it to start local tasks
			parsedConf.getJSONObject("param").put("repo", Constants.Base.getWorkerRepository()); //change repo for workers
		}
		
		
		private void startRemoteTask() throws Exception{
			String url = address + WORKER_JOB_PATH + "?action=submit";
			try{
				ticketToWorker(properties);
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

		private JobState retryStatus(){
			retries -= 1;
			if (retries < 0){
				return JobState.FAILED;
			}else{
				return JobState.RUNNING;
			}
		}
		
		private JobState getRemoteTaskStatus(){
			try {
				JSONObject remoteResult = JSONObject.parseObject(HttpClientUtil.get().doGet(address + WORKER_JOB_PATH + "?action=info&jobName=" + remoteJobName));
				if (remoteResult.containsValue("null")){ //remote task is not started yet
					return retryStatus();
				}
				String jobStateString = remoteResult.getJSONObject("response").getString("jobState");
				JobState state = FelucaJob.parseStateText(jobStateString);
				if (state == null ){
					return retryStatus();
				}else{
					retries = 2;
				}
				return state;
			} catch (Exception e) {
				return retryStatus();
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
