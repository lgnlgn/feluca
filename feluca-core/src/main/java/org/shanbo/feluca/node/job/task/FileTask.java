package org.shanbo.feluca.node.job.task;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.node.job.TaskExecutor;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * do file upload/download task
 * @Description TODO use additional process
 *	@author shanbo.liang
 */
public class FileTask extends TaskExecutor{

	HashMap<String, Boolean> fileNames;
	String ftpAddress;
	boolean isDownload = true;
	public FileTask(JSONObject conf) {
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
			isDownload = param.getBoolean("down");
		} catch (Exception e) {
			throw new FelucaException("fdfs address not found!", e);
		}
	}



	@Override
	public String getTaskName() {
		return "file";
	}

	@Override
	protected void _exec() {
		state = JobState.RUNNING;
		System.out.println("----------run :" + taskID );
		DataClient client = null;
		try {
			client = new DataFtpClient(ftpAddress.split(":")[0]);
			System.out.println("...........client opened......................");
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
				if (isDownload){
					System.out.println("downloading " + fileName);
					client.downFromRemote(fileName, Constants.Base.getWorkerRepository());
				}else{
					System.out.println("uploading " + fileName);
					File toCopy =  new File( Constants.Base.getWorkerRepository() + "/" + fileName);
					client.copyToRemote( toCopy.getParent() == null ? "./" : toCopy.getParent(), toCopy);
				}
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


	@Override
	public void kill() {
		state = JobState.STOPPING;
	}

	@Override
	public String getTaskFinalMessage() {
		return fileNames.toString();
	}


}
