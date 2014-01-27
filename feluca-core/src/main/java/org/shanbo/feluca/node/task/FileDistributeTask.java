package org.shanbo.feluca.node.task;

import java.io.IOException;
import java.util.HashMap;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @Description TODO use additional process
 *	@author shanbo.liang
 */
public class FileDistributeTask extends TaskExecutor{

	HashMap<String, Boolean> fileNames;
	String ftpAddress;
	
	public FileDistributeTask(JSONObject conf) {
		super(conf);
	}

	@Override
	protected void init(JSONObject initConf) {
		JSONObject param = initConf.getJSONObject("param");
		try {
			ftpAddress = param.getString("ftpAddress")== null?ClusterUtil.getFDFSAddress(): param.getString("ftpAddress");
			JSONArray files =  param.getJSONArray("files");
			fileNames = new HashMap<String, Boolean>();
			for(int i = 0; i < files.size();i++){
				fileNames.put(files.getString(i), false);
			}
		} catch (Exception e) {
			throw new FelucaException("fdfs address not found!", e);
		}
	}

	@Override
	public boolean isLocalJob() {
		return false;
	}


	@Override
	public String getTaskName() {
		return "file";
	}

	@Override
	public void execute() {
		ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				state = JobState.RUNNING;
				System.out.println("----------run :" + taskID );
				DataClient client = null;
				try {
					client = new DataFtpClient(ftpAddress.split(":")[0]);
				} catch (Exception e) {
					System.out.println("...........client failed......................");
					state = JobState.FAILED;
					return;
				}
				for(String fileName : fileNames.keySet()){
					if (state == JobState.STOPPING){
						break;
					}
					try {
						System.out.println("pulling " + fileName);
						client.downFromRemote(fileName, Constants.Base.getWorkerRepository());
						fileNames.put(fileName, true); //mark success
					} catch (IOException e) {
						log.error("downFromRemote error",e);
					}
				}
				if (client != null){
					client.close();
				}
				if (state == JobState.STOPPING){
					state = JobState.INTERRUPTED;
				}else
					state = JobState.FINISHED;
				System.out.println("-----------awake~~~~~~~~ " + state);
			}
		});
	}

	@Override
	public void kill() {
		state = JobState.STOPPING;
	}

	@Override
	protected JSONArray localTypeSubJob(JSONObject global) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject conf = reformNewConf(true);
		JSONObject param  = global.getJSONObject("param");
		if (param != null)
			conf.getJSONObject("param").putAll(param); //using user-def's parameter
		concurrentLevel.add(conf);
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	protected JSONArray distribTypeSubJob(JSONObject global) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();// all worker
		for(String worker : ClusterUtil.getWorkerList()){
			JSONObject conf = reformNewConf(false);
			conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);
			JSONObject param  = global.getJSONObject("param");
			if (param != null)
				conf.getJSONObject("param").putAll(param); //using user-def's parameter
			concurrentLevel.add(conf);
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getTaskFinalMessage() {
		return fileNames.toString();
	}

}
