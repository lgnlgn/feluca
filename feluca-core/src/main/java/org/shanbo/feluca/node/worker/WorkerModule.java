package org.shanbo.feluca.node.worker;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.job.FelucaJob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class WorkerModule extends RoleModule{
	private String dataDir; //
	
	private JobManager jobManager;
	
	public WorkerModule(){
		this.dataDir = Constants.Base.getWorkerRepository() + Constants.Base.DATA_DIR;
		this.jobManager = new JobManager();
		
		new File(Constants.Base.getWorkerRepository() + Constants.Base.DATA_DIR).mkdirs();
		new File(Constants.Base.getWorkerRepository() + Constants.Base.MODEL_DIR).mkdirs();
		new File(Constants.Base.getWorkerRepository() + Constants.Base.RESOURCE_DIR).mkdirs();

	}
	
	
	/**
	 * TODO
	 * @param dataName
	 * @return
	 */
	public List<String> listDataBlocks(String dataName){
		File dataSet = new File(dataDir + "/" + dataName);
		if (dataSet.isDirectory()){
			String[] segments = dataSet.list();
			Pattern pattern = Pattern.compile(dataName + "_(\\d+)\\.dat");
			ArrayList<String> list = new ArrayList<String>();
			for(String name : segments){
				 Matcher matcher = pattern.matcher(name);
				 if (matcher.find()){
					 list.add(matcher.group(1));
				 }
			}
			return list;
		}else{
			return Collections.emptyList();
		}
	}

	/**
	 * 
	 * @return
	 */
	public List<String> listDataSets(){
		String[] datasets =  new File(dataDir).list();
		return Arrays.asList(datasets);
		
	}
	
	
	public String getJobStatus(){
		return jobManager.getCurrentJobState();
	}
	
	public JSONObject searchJobInfo(String jobName){
		return this.jobManager.searchJobInfo(jobName);
	}
	
	
	public String submitJob(Class<? extends FelucaJob> clz, JSONObject conf) throws Exception{
		Constructor<? extends FelucaJob> constructor = clz.getConstructor(JSONObject.class);
		FelucaJob job = constructor.newInstance(conf);
		if (job.isLegal())
			return this.jobManager.asynRunJob(job);
		return null;
	}
	
	public String killJob(String jobName){
		if (StringUtils.isBlank(jobName))
			return "jobName empty!?";
		return 
			this.jobManager.killJob(jobName);
	}
	
	public JSONArray getLatestJobStates() {
		return this.jobManager.getLatestJobStates(1);
	}
	
	
	public static void main(String[] args) throws IOException {
		ProcessBuilder pb = new ProcessBuilder("java", "test1");
		Process p = pb.start();
		
	}
}
