package org.shanbo.feluca.node.worker.job;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.node.FelucaJob;
import org.shanbo.feluca.node.FelucaTask;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class PullDataJob extends FelucaJob{

	private  static  class PullTask extends FelucaTask{

		String fdfs ;
		String dataName;
		JSONArray blocks ;
		public PullTask(JSONObject prop) {
			super(prop);
			//TODO
		}

		@Override
		protected Runnable createStoppableTask() {
			Runnable r =  new Runnable() {

				public void run() {
					new File(Constants.Base.WORKER_DATASET_DIR + "/" + dataName).mkdir();
					DataClient dataClient = null;
					try{
						log.debug("open  dataclient");
						dataClient = new DataFtpClient(fdfs.split(":")[0]);
					}catch(IOException e){
						log.error("open  dataclient IOException", e);
						logError("open  dataclient IOException", e);
						state = JobState.FAILED;
						return;
					}
					int finishedBlocks = 0;
					for(int i = 0 ; i < blocks.size(); i++){
						if (state == JobState.STOPPING){
							break;
						}
						try {
							boolean ok= dataClient.downFromRemote(Constants.Base.DATA_DIR + "/"+ blocks.getString(i), Constants.Base.WORKER_DATASET_DIR + "/" + dataName);
							if (ok){
								finishedBlocks +=1;
							}
						} catch (IOException e) {
							logError("download " + blocks.getString(i) + " error ", e);
						}
					}
					if (finishedBlocks == blocks.size()){
						state = JobState.FINISHED;
					}else if (state == JobState.STOPPING){
						state = JobState.INTERRUPTED;
					}else {
						state = JobState.FAILED;
					}
				}
			};
			return r;
		}

	}



	public PullDataJob(JSONObject prop) {
		super(prop);
		this.jobName = "pullingJob";
		PullTask puller = new PullTask(prop);
		puller.setLogPipe(this.logPipe);
		this.addSubJobs(puller);
	}

	@Override
	protected String getAllLog() {
		return StringUtils.join(this.logPipe.iterator(), "");
	}

}
