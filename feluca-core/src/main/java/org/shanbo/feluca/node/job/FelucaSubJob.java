package org.shanbo.feluca.node.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.DistributedRequester;
import org.shanbo.feluca.util.Strings;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONObject;

/**
 * task is a special kind of job, i.e. , leaf of the leader's job-tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaSubJob extends FelucaJob{

	private TaskExecutor taskExecutor;
	
	protected boolean canSubJobGo = false;
	
	public FelucaSubJob(JSONObject prop) {
		super(prop);
		init();
		this.properties.putAll(taskExecutor.parseConfForSubJob());
	}
	
	protected abstract void estimateEnvForTask();

	
	private void init(){
		String task = this.properties.getString("task");
		this.taskExecutor = TASKS.get(task);
		estimateEnvForTask();

	}
	
	
	
	
	public boolean canSubJobRun() {
		return canSubJobGo;
	}
	

	abstract protected Runnable createStoppableTask();

	public void stopJob(){
		if (state == JobState.RUNNING || state == JobState.PENDING)
			state = JobState.STOPPING;
	}

	public void startJob(){
		state = JobState.RUNNING;
		ConcurrentExecutor.submit(createStoppableTask());
	}

	
	protected final void createTasks(){;}
	
	
	/**
	 * default log
	 */
	protected String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
	}
	
	
	public static class LocalSubJob extends FelucaSubJob{

		public LocalSubJob(JSONObject prop) {
			super(prop);
		}

		@Override
		protected void estimateEnvForTask() {
			
		}

		@Override
		protected Runnable createStoppableTask() {
			return null;
		}

		@Override
		protected boolean checkConfig(JSONObject para) {
			return false;
		}
		
	}
	
	
	/**
	 *  Distributed subjobs are allow sequentially  processed;
	 *  @Description TODO
	 *	@author shanbo.liang
	 */
	public static class DistribSeqSubJob extends FelucaSubJob{

		public DistribSeqSubJob(JSONObject prop) {
			super(prop);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected void estimateEnvForTask() {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected Runnable createStoppableTask() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected boolean checkConfig(JSONObject para) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
	
	/**
	 * Distributed subjobs: 1. send action to all(maybe) workers; 2. start check worker job; 
	 * example: file block download. 
	 *  @Description TODO
	 *	@author shanbo.liang
	 */
	public static class DistribSubJob extends FelucaSubJob{

		public DistribSubJob(JSONObject prop) {
			super(prop);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected void estimateEnvForTask() {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected Runnable createStoppableTask() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected boolean checkConfig(JSONObject para) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
	
	
	/**
	 * 
	 *  @Description TODO
	 *	@author shanbo.liang
	 */
	public static class SupervisorTask extends FelucaSubJob{

		JSONObject toSend = new JSONObject();
		List<String> ips ;
		Map<String, StateBag> ipJobStatus ;
		String taskName ;
		
		public static class StateBag{
			int retries = 1;
			JobState jobState = null;
			
			public StateBag(JobState js){
				this.jobState = js;
			}
			
		}
		
		public SupervisorTask(JSONObject prop) {
			super(prop);
			toSend.putAll(prop);
			taskName = toSend.getString("taskName");
			this.ips.addAll(prop.getJSONObject("ipAction").keySet());
			for(String ip : ips){
				ipJobStatus.put(ip, new StateBag(JobState.PENDING));
			}
		}


		protected Runnable createStoppableTask() {

			Runnable r = new Runnable() {			
				
				private boolean allSuccess(List<String> results){
					for(String result : results){
						JSONObject jsonObject = JSONObject.parseObject(result);
						if (jsonObject.getIntValue("code") >= 400){
							return false;
						}
					}
					return true;
				}
				
				private JobState checkAllPulses(List<String> results){
					for(int i = 0 ; i < ips.size(); i++){
						JSONObject jsonObject = JSONObject.parseObject(results.get(i));
						StateBag stateBag = ipJobStatus.get(ips.get(i));
						if (jsonObject.getIntValue("code") >= 400){
							stateBag.retries -= 1;
						}else{
							String js = jsonObject.getJSONObject(HttpResponseUtil.RESPONSE).getString("jobState");
							stateBag.jobState = FelucaJob.parseText(js);
						}
					}
					List<JobState> currentStates = new ArrayList<FelucaJob.JobState>();
					for(StateBag stateBag : ipJobStatus.values()){
						if (stateBag.retries >= 0){
							currentStates.add(stateBag.jobState);
						}
					}
					return FelucaJob.evaluateJobState(currentStates);
				}
				

				public void run() {
					List<String> broadcast = Collections.emptyList();
					try {
						broadcast = DistributedRequester.get().broadcast(toSend.getString("taskPath"),
									Strings.addNetworkCipherText(toSend), ips);
					} catch (Exception e1) {
					} 
					if (allSuccess(broadcast)){
						int action = 0;
						long tStart = DateUtil.getMsDateTimeFormat();
						while(true){
							if (state == JobState.STOPPING){
								try {
									DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
											Strings.kvNetworkMsgFormat("",""), ips);
									Thread.sleep(100);
									continue;
								} catch (Exception e) {
								}
							}
							try {
								List<String> currentWorkerStatus= DistributedRequester.get().broadcast("/jobStates" + taskName,
										Strings.kvNetworkMsgFormat("",""), ips);
								JobState workerState = checkAllPulses(currentWorkerStatus);
								long elapse = DateUtil.getMsDateTimeFormat() - tStart;
								if (action == 0 && ttl > 0 && elapse > ttl){
									DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
											Strings.kvNetworkMsgFormat("",""), ips);
									action = 1;
									log.debug("too long, send kill job request to workers!");
									//then wait for JobState.FINISHED
								}
								if (workerState == JobState.FINISHED){
									finishTime = DateUtil.getMsDateTimeFormat();
									log.debug("sub jobs finished");
									state = JobState.FINISHED;
									break;
								}else if (workerState == JobState.INTERRUPTED){
									finishTime = DateUtil.getMsDateTimeFormat();
									log.debug("sub jobs interrupted");
									state = JobState.INTERRUPTED;
									break;
								}else if (workerState == JobState.FAILED){
									finishTime = DateUtil.getMsDateTimeFormat();
									log.debug("sub jobs faild");
									state = JobState.FAILED;
									break;
								}
								log.debug("checking~~~~workers : " + workerState);
								Thread.sleep(300);
							}catch (Exception e) {
							}
							
						}
					}else{ //start worker job failed?????? 
						try {
							DistributedRequester.get().broadcast("/kill?jobName=" + taskName,
									Strings.kvNetworkMsgFormat("",""), ips);
						} catch (Exception e) {
						} 
					}
				}
			};
			return r;
		}


		@Override
		protected boolean checkConfig(JSONObject para) {
			return isLegal;
		
		}

	}

}
