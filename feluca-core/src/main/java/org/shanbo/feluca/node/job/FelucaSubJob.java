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
	protected JobState state ;
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
		String taskClass = this.properties.getString("task");
		try {
			Class<? extends TaskExecutor> clz = (Class<? extends TaskExecutor>) Class.forName(taskClass);
			Constructor<? extends TaskExecutor> constructor = clz.getConstructor(JSONObject.class);
			taskExecutor = constructor.newInstance(this.properties);
		} catch (Exception e) {
			log.error("init error");
		}
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
	protected void init(){

	}

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
							state = taskExecutor.currentState();
							if (state == JobState.FAILED || state== JobState.INTERRUPTED || state == JobState.FINISHED){
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

	}
	
	
	//TODO
	public static class DistributeSubJob extends FelucaSubJob{
		final static String WORKER_JOB_PATH = "/job";
		
		String address;
		String remoteJobName ;
		int retries = 2;
		
		private void startRemoteTask() throws Exception{
			try{
				remoteJobName = HttpClientUtil.get().doPost(address + WORKER_JOB_PATH + "?action=submit", properties.toJSONString());
			}catch (Exception e){
				Thread.sleep(2000);
				remoteJobName = HttpClientUtil.get().doPost(address + WORKER_JOB_PATH + "?action=submit", properties.toJSONString());
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
							state = getRemoteTaskStatus();
							if (state == JobState.FAILED || state== JobState.INTERRUPTED || state == JobState.FINISHED){
								break;
							}
						}
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
	}


	/**
	 * 
	 *  @Description TODO
	 *	@author shanbo.liang
	 */
	//	public static class SupervisorTask extends FelucaSubJob{
	//
	//		JSONObject toSend = new JSONObject();
	//		List<String> ips ;
	//		Map<String, StateBag> ipJobStatus ;
	//		String taskName ;
	//		
	//		public static class StateBag{
	//			int retries = 1;
	//			JobState jobState = null;
	//			
	//			public StateBag(JobState js){
	//				this.jobState = js;
	//			}
	//			
	//		}
	//		
	//		public SupervisorTask(JSONObject prop) {
	//			super(prop);
	//			toSend.putAll(prop);
	//			taskName = toSend.getString("taskName");
	//			this.ips.addAll(prop.getJSONObject("ipAction").keySet());
	//			for(String ip : ips){
	//				ipJobStatus.put(ip, new StateBag(JobState.PENDING));
	//			}
	//		}
	//
	//
	//		public Runnable createStoppableTask() {
	//
	//			Runnable r = new Runnable() {			
	//				
	//				private boolean allSuccess(List<String> results){
	//					for(String result : results){
	//						JSONObject jsonObject = JSONObject.parseObject(result);
	//						if (jsonObject.getIntValue("code") >= 400){
	//							return false;
	//						}
	//					}
	//					return true;
	//				}
	//				
	//				private JobState checkAllPulses(List<String> results){
	//					for(int i = 0 ; i < ips.size(); i++){
	//						JSONObject jsonObject = JSONObject.parseObject(results.get(i));
	//						StateBag stateBag = ipJobStatus.get(ips.get(i));
	//						if (jsonObject.getIntValue("code") >= 400){
	//							stateBag.retries -= 1;
	//						}else{
	//							String js = jsonObject.getJSONObject(HttpResponseUtil.RESPONSE).getString("jobState");
	//							stateBag.jobState = FelucaJob.parseText(js);
	//						}
	//					}
	//					List<JobState> currentStates = new ArrayList<FelucaJob.JobState>();
	//					for(StateBag stateBag : ipJobStatus.values()){
	//						if (stateBag.retries >= 0){
	//							currentStates.add(stateBag.jobState);
	//						}
	//					}
	//					return FelucaJob.evaluateJobState(currentStates);
	//				}
	//				
	//
	//				public void run() {
	//					List<String> broadcast = Collections.emptyList();
	//					try {
	//						broadcast = DistributedRequester.get().broadcast(toSend.getString("taskPath"),
	//									Strings.addNetworkCipherText(toSend), ips);
	//					} catch (Exception e1) {
	//					} 
	//					if (allSuccess(broadcast)){
	//						int action = 0;
	//						long tStart = DateUtil.getMsDateTimeFormat();
	//						while(true){
	//							if (state == JobState.STOPPING){
	//								try {
	//									DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
	//											Strings.kvNetworkMsgFormat("",""), ips);
	//									Thread.sleep(100);
	//									continue;
	//								} catch (Exception e) {
	//								}
	//							}
	//							try {
	//								List<String> currentWorkerStatus= DistributedRequester.get().broadcast("/jobStates" + taskName,
	//										Strings.kvNetworkMsgFormat("",""), ips);
	//								JobState workerState = checkAllPulses(currentWorkerStatus);
	//								long elapse = DateUtil.getMsDateTimeFormat() - tStart;
	//								if (action == 0 && ttl > 0 && elapse > ttl){
	//									DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
	//											Strings.kvNetworkMsgFormat("",""), ips);
	//									action = 1;
	//									log.debug("too long, send kill job request to workers!");
	//									//then wait for JobState.FINISHED
	//								}
	//								if (workerState == JobState.FINISHED){
	//									finishTime = DateUtil.getMsDateTimeFormat();
	//									log.debug("sub jobs finished");
	//									state = JobState.FINISHED;
	//									break;
	//								}else if (workerState == JobState.INTERRUPTED){
	//									finishTime = DateUtil.getMsDateTimeFormat();
	//									log.debug("sub jobs interrupted");
	//									state = JobState.INTERRUPTED;
	//									break;
	//								}else if (workerState == JobState.FAILED){
	//									finishTime = DateUtil.getMsDateTimeFormat();
	//									log.debug("sub jobs faild");
	//									state = JobState.FAILED;
	//									break;
	//								}
	//								log.debug("checking~~~~workers : " + workerState);
	//								Thread.sleep(300);
	//							}catch (Exception e) {
	//							}
	//							
	//						}
	//					}else{ //start worker job failed?????? 
	//						try {
	//							DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
	//									Strings.kvNetworkMsgFormat("",""), ips);
	//						} catch (Exception e) {
	//						} 
	//					}
	//				}
	//			};
	//			return r;
	//		}
	//
	//
	//
	//	}

}
