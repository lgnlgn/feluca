package org.shanbo.feluca.node.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.common.FelucaJob.JobState;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.util.ElementPicker;
import org.shanbo.feluca.util.ZKClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * TODO
 * @author lgn
 *
 */
public class DataDispatchJob extends FelucaJob{


	private String dataDir;
	static class DeliveryJob extends FelucaJob{

		static final long FILESIZEMAX_ONELOOP = 88 * 1024* 1024;
		static final long FILENUMMAX_ONELOOP = 6;

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
					for(String ip : ipFiles.keySet()){
						JSONArray files = ipFiles.getJSONArray(ip);
						int num = 0;
						long sizesum = 0;
						DataClient dataClient = null;
						try{
							dataClient = new DataFtpClient(ip.split(":")[0]);
							dataClient.makeDirecotry(dataName);
						}catch(IOException e){
							log.error("data client IOException", e);
							continue;
						}
						for(int i = 0 ; i < files.size(); i++){
							String fileName = files.getString(i);
							try{
								RandomAccessFile fis = new RandomAccessFile(fileName, "r");
								sizesum += fis.length();
								num += 1;
								fis.close();

								if (num >= FILESIZEMAX_ONELOOP || sizesum >= FILESIZEMAX_ONELOOP){
									if (state == JobState.INTERRUPTED){
										dataClient.close();
										return ;
									}
								}else{
									dataClient.copyToRemote(dataName, new File(dataName + "/" + fileName));
								}
							}catch(IOException e){
								log.error("delivery ioe", e);

							}
						}
						dataClient.close();
					}
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
		taskProperties.put("jobName", "delivery_" + System.currentTimeMillis());
		taskProperties.put("dataName", dataName);
		return taskProperties;
	}

	public DataDispatchJob(Properties prop) {
		super(prop);
		this.dataDir = prop.getProperty("dataDir", Constants.DATA_PATH);
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
		List<String> nodes = ZKClient.get().getChildren(Constants.FDFS_ZK_ROOT);
		if (nodes.isEmpty() || StringUtils.isBlank(dataName))
			return Collections.emptyMap();
		File dataSet = new File(this.dataDir + "/" + dataName);
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
			return resultMap;
		}else{
			return Collections.emptyMap();
		}
	}

}
