package org.shanbo.feluca.node.worker;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.job.FelucaJob;

import com.alibaba.fastjson.JSONObject;

public class WorkerModule extends RoleModule{
	private String dataDir; //
	
	private JobManager jobManager;
	
	public WorkerModule(){
		this.dataDir = Constants.Base.WORKER_DATASET_DIR;
	}
	
	
	/**
	 * TODO
	 * @param dataName
	 * @return
	 */
	public List<String> listDataBlocks(String dataName){
		return null;
	}

	/**
	 * 
	 * @return
	 */
	public List<String> listDataSets(){
		return null;
	}
	
	
	public String getJobStatus(){
		return jobManager.getCurrentJobState();
	}
	
	public JSONObject searchJobInfo(String jobName){
		return this.jobManager.searchJobInfo(jobName);
	}
	
	
	public String submitJob(Class<? extends FelucaJob> clz, JSONObject conf) throws Exception{
		if (clz == null)
			return null;
		return this.jobManager.asynRunJob(clz, conf);
	}
	
	public String killJob(String jobName){
		if (StringUtils.isBlank(jobName))
			return "jobName empty!?";
		return 
			this.jobManager.killJob(jobName);
	}
	
	
	public static void main(String[] args) throws IOException {
		ProcessBuilder pb = new ProcessBuilder("java", "test1");
		Process p = pb.start();
		
	}
}
