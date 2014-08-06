package org.shanbo.feluca.distribute.launch;

import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.util.ZKUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class StartingGun2 implements Closeable{
	
	private int totalWorkers; 
	private int totalReduceServers;
	private  String path;
	
	CuratorFramework client ;
	
	public StartingGun2(String taskName, int totalReduceServers, int totalWorkers) {
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.totalWorkers = totalWorkers;
		this.totalReduceServers = totalReduceServers;
		client = ZKUtils.newClient();

	}
	
	public void startAndWait() throws Exception{
		System.out.println("reducer:" + totalReduceServers + " workers:" + totalWorkers);
		client.start();
		if (client.checkExists().forPath(path + Constants.Algorithm.ZK_REDUCER_PATH) == null)
			client.create().creatingParentsIfNeeded().forPath(path + Constants.Algorithm.ZK_REDUCER_PATH);
		if (client.checkExists().forPath(path + Constants.Algorithm.ZK_MODELSERVER_PATH) == null)
			client.create().creatingParentsIfNeeded().withProtection().forPath(path + Constants.Algorithm.ZK_MODELSERVER_PATH);
		ConcurrentExecutor.submitAndWait(new Runnable() {
			public void run() {
				try{
					int currentReducer = client.getChildren().forPath(path + Constants.Algorithm.ZK_REDUCER_PATH).size();
					int currentWorker = client.getChildren().forPath(path + Constants.Algorithm.ZK_MODELSERVER_PATH).size();
					while( currentReducer < totalReduceServers
						|| currentWorker < totalWorkers){
						try{
							Thread.sleep(10);
						}catch (InterruptedException e) {
							break;
						}
						currentReducer = client.getChildren().forPath(path + Constants.Algorithm.ZK_REDUCER_PATH).size();
						currentWorker = client.getChildren().forPath(path + Constants.Algorithm.ZK_MODELSERVER_PATH).size();

					}
				} catch (Exception e) {
					throw new FelucaException("waitFor allServerStarted Exception ",e );
				} 
			}
		}, 100000);
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path + "/start");
	}
	
	public void setFinish() throws Exception{
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path + "/finish");
	}
	
	public void close() throws IOException{
		client.close();
	}
	
}
