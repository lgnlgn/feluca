package org.shanbo.feluca.node.leader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.ZKClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class LeaderModule extends RoleModule{
	
	private JobManager distributeJobManager;
	private JobManager localJobManager;
	private String dataDir;
	

	public LeaderModule() throws KeeperException, InterruptedException{
		dataDir = Constants.Base.getLeaderRepository() +  Constants.Base.DATA_DIR;
		ZKClient.get().createIfNotExist(Constants.Base.ZK_LEADER_PATH);
		ZKClient.get().createIfNotExist(Constants.Base.ZK_WORKER_PATH);
		this.distributeJobManager = new JobManager();
		this.localJobManager = new JobManager();
		
		new File(Constants.Base.getLeaderRepository() + Constants.Base.DATA_DIR).mkdirs();
		new File(Constants.Base.getLeaderRepository() + Constants.Base.MODEL_DIR).mkdirs();
		new File(Constants.Base.getLeaderRepository() + Constants.Base.RESOURCE_DIR).mkdirs();
	}

	

	
	public String getDataDir(){
		return this.dataDir;
	}
	
	
	public boolean dataSetExist(String dataName){
		return new File(dataDir + "/" + dataName).isDirectory();
	}
	
	
	public String submitJob(Class<? extends FelucaJob> clz, JSONObject conf) throws Exception{
		Constructor<? extends FelucaJob> constructor = clz.getConstructor(JSONObject.class);
		FelucaJob job = constructor.newInstance(conf);
		if (job.isLegal()){
			if (job.isLocal())
				return this.localJobManager.asynRunJob(job);
			else
				return this.distributeJobManager.asynRunJob(job);
		}else{
			return null;
		}
		
	}
	

	
	public String killJob(String jobName, boolean isLocal){
		if (StringUtils.isBlank(jobName))
			return "jobName empty!?";
		if (isLocal)
			return this.localJobManager.killJob(jobName);
		else 
			return this.distributeJobManager.killJob(jobName);
	}
	
	
	public JSONObject searchJobInfo(String jobName, boolean isLocal){
		if (isLocal)
			return this.localJobManager.searchJobInfo(jobName);
		return this.distributeJobManager.searchJobInfo(jobName);
	}
	
	public JSONArray getLatestJobStates(int size ,boolean isLocal) {
		if (isLocal)
			return this.localJobManager.getLatestJobStates(size);
		return this.distributeJobManager.getLatestJobStates(size);
	}
	
	
	
	/**
	 * TODO
	 * @return
	 */
	public JSONArray localDataSet(){
		return JSONUtil.fromStrings(new File(dataDir).list());
	}
	
	/**
	 * TODO
	 * @return
	 * @throws IOException 
	 */
	public JSONObject localDataSetInfo(String dataName) throws IOException{
		Properties dataStatus = FileUtil.loadProperties(dataDir + "/" + dataName + "/" + dataName + ".sta");
		return JSONUtil.fromProperties(dataStatus);
	}
	
	/**
	 * //merge all workers dataSets
	 *  TODO
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static JSONArray showClusterDataSets() throws InterruptedException, ExecutionException{
		List<String> workers = ClusterUtil.getWorkerList();
		Map<String, String> result = HttpClientUtil.distribGet(workers, "/state?type=data");
		//TODO  intact checking
		HashSet<String> dataSets = new HashSet<String>();
		for(String responseString : result.values()){
			JSONObject jo = JSONObject.parseObject(responseString);
			dataSets.addAll(JSONUtil.JSONArrayToList(jo.getJSONArray("response")));
		}
		return JSONUtil.listToAJsonArray(dataSets);
	}
	
	/**
	 * 
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static JSONObject showClusterDataInfo(String dataName) throws InterruptedException, ExecutionException{
		//TODO 
		List<String> workers = ClusterUtil.getWorkerList();
		Map<String, String> result = HttpClientUtil.distribGet(workers, "/state?type=data?dataName="+ dataName);
		//TODO  intact checking
		JSONObject invert = new JSONObject();
		for(Entry<String, String> addressAndBlocks: result.entrySet()){
			JSONArray blocks = JSONObject.parseObject(addressAndBlocks.getValue()).getJSONArray("response");
			for(Object block : blocks){
				JSONArray addresses = invert.getJSONArray(block.toString());
				if (addresses == null){
					addresses = new JSONArray();
					invert.put(block.toString(), addresses);
				}
				addresses.add(addressAndBlocks.getKey());
			}
		}
		return invert;
	}
	
	
}
