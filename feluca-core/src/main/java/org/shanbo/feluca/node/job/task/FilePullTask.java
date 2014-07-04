package org.shanbo.feluca.node.job.task;

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
 * files are spread to all workers 
 * @Description TODO use additional process
 *	@author shanbo.liang
 */
public class FilePullTask extends TaskExecutor{

	HashMap<String, Boolean> fileNames;
	String ftpAddress;

	public FilePullTask(JSONObject conf) {
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
	public String getTaskName() {
		return "filepull";
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


	@Override
	public void kill() {
		state = JobState.STOPPING;
	}

	@Override
	public String getTaskFinalMessage() {
		return fileNames.toString();
	}


}