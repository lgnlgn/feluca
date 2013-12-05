package org.shanbo.feluca.node.worker.job;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.datasys.ftp.DataFtpClient;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class PullDataJob extends FelucaJob{

	public static class Puller extends FelucaJob{

		String leaderAddress;
		String[] blocks;
		String dataName;
		String dataDir;
		public Puller(Properties prop) {
			super(prop);
			blocks = prop.getProperty("blocks", "").split(",");
			dataName = prop.getProperty("dataName");
			dataDir = prop.getProperty("data");
		}

		@Override
		protected String getAllLog() {
			return null;
		}

		private boolean canJobRun(){
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

		public void stopJob(){
			state = JobState.STOPPING;
		}

		public void startJob(){
			state = JobState.RUNNING;
			ConcurrentExecutor.submit(new Runnable() {
				public void run() {
					int finishedBlocks = 0;
					if (!canJobRun()){
						state = JobState.FAILED;
						logError("", new FelucaException("parameters maybe lost~~~"));
					}
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
					if (state == JobState.STOPPING){
						state = JobState.INTERRUPTED;
					}else if (finishedBlocks == blocks.length){
						state = JobState.FINISHED;
					}else{
						state = JobState.FAILED;
					}

				}
			});
		}

	}

	public PullDataJob(Properties prop) {
		super(prop);
		this.jobName = "pullingJob";
		Puller puller = new Puller(prop);
		puller.setLogPipe(this.logPipe);
		this.addSubJobs(puller);
	}

	@Override
	protected String getAllLog() {

		return null;
	}

}
