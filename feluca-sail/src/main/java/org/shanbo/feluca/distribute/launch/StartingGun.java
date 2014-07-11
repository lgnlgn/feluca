package org.shanbo.feluca.distribute.launch;

import java.util.List;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * not seen by algorithms
 * 
 * TODO 
 * @author lgn
 *
 */
public class StartingGun {

	Logger log = LoggerFactory.getLogger(StartingGun.class);

	
	private ChildrenWatcher workerWatcher;
	
	private int totalWorkers; 
	private int waitingWorkers;
	private  String path;
	private int loop;
	
	public StartingGun(String taskName, int totalWorkers) {
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.totalWorkers = totalWorkers;

	}
	
	private void createZKPath() throws KeeperException, InterruptedException{
		ClusterUtil.createZKPaths(path);
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_LOOP_PATH); 
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_WAITING_PATH);
		
		
	}
	
	private void startWatch() {
		workerWatcher = new ChildrenWatcher() {
			public void nodeRemoved(String node) {
			}
			public void nodeAdded(String node) {
				waitingWorkers += 1;
				if (waitingWorkers  == totalWorkers){
					setSignal();
				}
			}
		};
		ZKClient.get().watchChildren(path + "/workers", workerWatcher);
	}
	
	private void setSignal(){
		waitingWorkers = 0;
		String workerPath = path + Constants.Algorithm.ZK_WAITING_PATH;
		try {
			List<String> waitingList = ZKClient.get().getChildren(workerPath);
			for(String workerNode : waitingList){
				ZKClient.get().forceDelete(workerPath + "/" + workerNode);
			}
			ZKClient.get().setData(path + Constants.Algorithm.ZK_LOOP_PATH, ("" + loop).getBytes());
		} catch (Exception e) {
			log.error("all workers are ready. but error here :" + loop, e);
		}
		loop += 1;
	}
	
	public void start() throws KeeperException, InterruptedException{
		this.createZKPath();
		this.startWatch();
	}
	
	public void close() throws InterruptedException{
		ZKClient.get().destoryWatch(workerWatcher);

	}
	
	public void wait(Runnable runnable) throws InterruptedException{
		Future<?> submit = ConcurrentExecutor.submit(runnable);
		submit.wait(5000);
	}
	
	public void setFinish() throws KeeperException, InterruptedException{
		ZKClient.get().setData(path, "finish".getBytes());
	}
	
	
	public String toString() {
		return path + " of (" + totalWorkers+ ")" ;
	}
	
}
