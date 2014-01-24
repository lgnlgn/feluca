package org.shanbo.feluca.node.leader;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class LeaderModule extends RoleModule{
	
	private JobManager distributeJobManager;
	private JobManager localJobManager;
	private String dataDir;
	

	public LeaderModule() throws KeeperException, InterruptedException{
		dataDir = Constants.Base.LEADER_DATASET_DIR;
		ZKClient.get().createIfNotExist(Constants.Base.ZK_LEADER_PATH);
		ZKClient.get().createIfNotExist(Constants.Base.ZK_WORKER_PATH);
		this.distributeJobManager = new JobManager();
		this.localJobManager = new JobManager();
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
	public JSONObject localDataSet(){
		return null;
	}
	
	/**
	 * TODO
	 * @return
	 */
	public JSONObject localDataSetInfo(String dataName){
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public JSONObject gatherRemoteDataSet(){
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public JSONObject gatherRemoteDataSetInfo(String dataName){
		return null;
	}
	
	
}
