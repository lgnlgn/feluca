package org.shanbo.feluca.node.worker.job;

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

import com.alibaba.fastjson.JSONObject;

public class PullDataJob extends FelucaJob{

	public static class PullTask extends FelucaTask{
		String leaderAddress;
		String[] blocks;
		String dataName;
		String dataDir;
		int finishedBlocks = 0;
		public PullTask(JSONObject prop) {
			super(prop);
			blocks = prop.getString("blocks").split(",");
			dataName = prop.getString("dataName");
			dataDir = prop.getString("data");
		}

		@Override
		protected boolean canTaskRun() {
			try {
				List<String> leaderAddesses= ZKClient.get().getChildren(Constants.Base.ZK_LEADER_PATH);
				if (leaderAddesses.isEmpty()){
					logError("", new FelucaException("leader dataserver not found"));
					return false;
				}
				this.leaderAddress = leaderAddesses.get(0);
			} catch (InterruptedException e) {
				logError("fetch leader address InterruptedException", e);
			} catch (KeeperException e) {
				logError("fetch leader address KeeperException", e);
			}
			if (StringUtils.isEmpty(leaderAddress) || StringUtils.isEmpty(dataName) 
					|| blocks.length == 0 || StringUtils.isEmpty(dataDir)){
				return false;
			}else
				return true;
		}

		@Override
		protected StoppableRunning createStoppableTask() {

			return new StoppableRunning() {

				@Override
				protected void runTask() {
					DataClient dataClient = null;
					try{
						log.debug("open :" + leaderAddress + " dataclient");
						dataClient = new DataFtpClient(leaderAddress.split(":")[0]);
						dataClient.makeDirecotry(dataName);
					}catch(IOException e){
						log.error("open :" + leaderAddress + " dataclient OR mkdir IOException", e);
						logError("open :" + leaderAddress + " dataclient OR mkdir IOException", e);
						state = JobState.FAILED;
					}
					for(String block : blocks){
						if (state == JobState.STOPPING){
							logInfo("interrupted!!!!!!!!!!!!!!!!");
						}
						String blockFile = dataDir + "/" + dataName + "/" + block;
						boolean downloaded;
						try {
							downloaded = dataClient.downFromRemote(blockFile, Constants.Base.WORKER_DATASET_DIR + "/" + dataName + "/" + block);
							if (!downloaded){
								downloaded = dataClient.downFromRemote(blockFile, Constants.Base.WORKER_DATASET_DIR + "/" + dataName + "/" + block);
							}
							if (downloaded){
								finishedBlocks += 1;
							}
						} catch (IOException e) {
							log.error("download " + blockFile + " failed", e);
							logError("download " + blockFile + " failed", e);
						}
					}
				}

				@Override
				protected boolean isTaskSuccess() {
					if (finishedBlocks == blocks.length){
						return true;
					}else {
						return false;
					}
				}
			};

		}

		@Override
		protected String getAllLog() {
			// TODO Auto-generated method stub
			return null;
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

		return null;
	}

}
