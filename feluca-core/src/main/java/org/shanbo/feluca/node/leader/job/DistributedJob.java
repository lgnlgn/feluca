package org.shanbo.feluca.node.leader.job;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.FelucaJob;
import org.shanbo.feluca.node.FelucaTask;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.DistributedRequester;
import org.shanbo.feluca.util.ElementPicker;
import org.shanbo.feluca.util.Strings;
import org.shanbo.feluca.util.ZKClient;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Collections2;

/**
 * TODO routing strategy , need broadcast 
 * 
 * @author lgn
 *
 */
public class DistributedJob extends FelucaJob{

	private String fromDir;

	private static class PullTaskKeeper extends FelucaTask{

		Map<String, List<String>> ipFiles;
		String taskName;
		public PullTaskKeeper(JSONObject prop) {
			super(prop);
			
		}

		@Override
		protected boolean canTaskRun() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		protected StoppableRunning createStoppableTask() {
			return  new StoppableRunning() {
				
				
				
				@Override
				protected void runTask() {
					try {
						DistributedRequester.get().broadcast("/job?jobName=PullData", 
								Strings.kvNetworkMsgFormat(FelucaJob.JOB_DETAIL, ipFiles, FelucaJob.JOB_NAME, taskName), 
								new ArrayList<String>(ipFiles.keySet()) );					 
					} catch (Exception e) {
						logError("send job failed?!!  we are going to stop", e);
						try {
							DistributedRequester.get().broadcast("/kill?jobName=" + taskName, 
									Strings.kvNetworkMsgFormat(FelucaJob.JOB_DETAIL, ""), 
									new ArrayList<String>(ipFiles.keySet()) );
						} catch (Exception e1) {
							logError("stop job failed?!! ", e1);
						} 	
					}
					while(true){
						if (state == JobState.STOPPING){
							try {
								DistributedRequester.get().broadcast("/kill?jobName=" + taskName, 
										Strings.kvNetworkMsgFormat(FelucaJob.JOB_DETAIL, ""), 
										new ArrayList<String>(ipFiles.keySet()) );
								
							} catch (Exception e) {
								logError("stop job failed?!! ", e);
							}
						}
						//TODO 
					}
				}
				
				@Override
				protected boolean isTaskSuccess() {
					// TODO Auto-generated method stub
					return false;
				}
			};
		}

		@Override
		protected String getAllLog() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	
	public JSONObject generateConf(String dataName,Map<String, List<String>> taskDetail){
		JSONObject jo = new JSONObject();
		jo.put("taskDetail", taskDetail);
		jo.put(FelucaJob.JOB_NAME, "delivery_" + DateUtil.getMsDateTimeFormat());
		jo.put("dataName", dataName);
		return jo;
	}
	
	
	public DistributedJob(JSONObject prop) {
		super(prop);

	}

	public String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
	}


	private Map<String, List<String>> allocateFiles(String dataName) throws InterruptedException, KeeperException{
		List<String> nodes = ZKClient.get().getChildren(Constants.Base.FDFS_ZK_ROOT);
		if (nodes.isEmpty() || StringUtils.isBlank(dataName))
			return Collections.emptyMap();
		File dataSet = new File(this.fromDir + "/" + dataName);
		if (dataSet.exists() && dataSet.isDirectory()){
			File[] segments = dataSet.listFiles();
			Map<String, List<String>> resultMap = new HashMap<String, List<String>>();//(ip:list(filename))
			Map<String, String> tmpMap = new HashMap<String, String>(); //(prefix:ip)

			for(String node : nodes){
				resultMap.put(node, new ArrayList<String>());
			}
			ElementPicker<String> picker = new ElementPicker.RoundRobinPicker<String>(resultMap.keySet());
			for(File segment : segments){
				String filePrefix = segment.getName().split("\\.")[0];
				String ip = tmpMap.get(filePrefix);
				if (ip == null){
					ip = picker.pick();
					tmpMap.put(filePrefix, ip);
				}
				List<String> listForCopyTo = resultMap.get(ip);
				listForCopyTo.add(segment.getName());
			}
			for(String node : nodes){
				if (resultMap.get(node).isEmpty()){
					resultMap.remove(node);
				}
			}
			log.debug("show map:" + resultMap.toString());
			return resultMap;
		}else{
			return Collections.emptyMap();
		}
	}

}
