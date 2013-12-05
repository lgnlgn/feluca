package org.shanbo.feluca.node.leader.job;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.ElementPicker;
import org.shanbo.feluca.util.ZKClient;

import com.alibaba.fastjson.JSONObject;

/**
 * TODO routing strategy , need broadcast 
 * 
 * @author lgn
 *
 */
public class WorkersDataPullJob extends FelucaJob{


	private String fromDir;
	static class DeliveryJob extends FelucaJob{
		JSONObject ipFiles ; //
		String dataName;
		Thread deliverer;
		public DeliveryJob(Properties prop) {
			super(prop);
			String taskDetail = prop.getProperty("taskDetail");
			ipFiles = JSONObject.parseObject(taskDetail);
			dataName = prop.getProperty("dataName");
		}

		@Override
		protected String getAllLog() {
			return StringUtils.join(this.logCollector.iterator(), "");
		}

		public void stopJob() {
			state = JobState.INTERRUPTED;
		}

		public void startJob(){
			state = JobState.RUNNING;
			deliverer = new Thread(new Runnable() {

				public void run() {
					
					state = JobState.FINISHED;
				}
			}, "delivery");
			deliverer.setDaemon(true);
			deliverer.start();
		}
	}


	private Properties generateProperties(String dataName,Map<String, List<String>> taskDetail){
		Properties taskProperties = new Properties();
		taskProperties.put("taskDetail", JSONObject.toJSONString(taskDetail));
		taskProperties.put(FelucaJob.JOB_NAME, "delivery_" + DateUtil.getMsDateTimeFormat());
		taskProperties.put("dataName", dataName);
		return taskProperties;
	}

	public WorkersDataPullJob(Properties prop) {
		super(prop);
		this.fromDir = prop.getProperty("dataDir", Constants.Base.WORKER_DATASET_DIR);
		String dataName = prop.getProperty("dataName");
		try {
			Map<String, List<String>> taskDetail = allocateFiles(dataName);
			Properties subJobProperties = generateProperties(dataName, taskDetail);
			FelucaJob delivery = new DeliveryJob(subJobProperties);
			delivery.setLogPipe(logPipe); 
			this.addSubJobs(delivery);
		} catch (InterruptedException e) {
			log.error("taskDetail fetch InterruptedException" ,e);
		} catch (KeeperException e) {
			log.error("taskDetail fetch KeeperException" ,e);
		}
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
