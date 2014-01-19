package org.shanbo.feluca.node.job;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONObject;

/**
 * task is a special kind of job, i.e. , leaf of the leader's job-tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaSubJob extends FelucaJob{

	final static int CHECK_TASK_INTERVAL_MS = 100;
	protected TaskExecutor taskExecutor;
	
	protected boolean canSubJobGo = false;
	
	public FelucaSubJob(JSONObject prop) {
		super(prop);
		init();
	}
	
	
	protected void init(){
		String task = this.properties.getString("task");
		this.taskExecutor = TASKS.get(task);
	}
	

	/**
	 * you must include taskrun and supervision
	 * @return
	 */
	public abstract  Runnable createStoppableTask();

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
			return new LocalSubJob(parsedConf.getJSONObject("conf"));
		}else{
			return new DistributeSubJob(parsedConf.getJSONObject("conf"));
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
							break;
						}
					}
				}
			};
		}
		
	}
	//TODO
	public static class DistributeSubJob extends FelucaSubJob{
		final static String WORKER_JOB_SUBMIT = "/job";
		final static String WORKER_JOB_STATUS = "/jobstatus";
		String address;
		
		private void startRemoteTask(){
			//TODO
		}
		
		
		private void killRemoteTask(){
			//TODO
		}
		
		private JobState getRemoteTaskStatus(){
			//TODO
			return null;
		}
		
		public DistributeSubJob(JSONObject prop) {
			super(prop);
		}

		@Override
		public Runnable createStoppableTask() {
			return new Runnable() {
				
				public void run() {
					startRemoteTask();
					boolean killed = false;
					while(true){
						if (killed == false && state == JobState.STOPPING){
							killRemoteTask();
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
