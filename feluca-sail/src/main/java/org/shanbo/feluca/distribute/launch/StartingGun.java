package org.shanbo.feluca.distribute.launch;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
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
	private int totalModelServers;
	private int waitingWorkers;
	private  String path;
	private int loop;
	
	public StartingGun(String taskName, int totalModelServers, int totalWorkers) {
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.totalWorkers = totalWorkers;
		this.totalModelServers = totalModelServers;

	}
	
	private void createZKPath() throws KeeperException, InterruptedException{
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
					setLoopSignal();
				}
			}
		};
		ZKClient.get().watchChildren(path + Constants.Algorithm.ZK_WAITING_PATH, workerWatcher);
	}
	
	private void setLoopSignal(){
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
	
	public void close() throws InterruptedException, KeeperException{
		ZKClient.get().destoryWatch(workerWatcher);
		ZKClient.get().forceDelete(path + Constants.Algorithm.ZK_LOOP_PATH);
		ZKClient.get().forceDelete(path + Constants.Algorithm.ZK_WAITING_PATH);
		System.out.println("startingGun of [" + path + "] closed"  );
	}
	
	public void submitAndWait(Runnable runnable) throws InterruptedException, ExecutionException, TimeoutException{
		submitAndWait(runnable, 5000);
	}
	
	public void waitForModelServersStarted() throws InterruptedException, ExecutionException, TimeoutException{
		submitAndWait(new Runnable() {
			public void run() {
				try {
					while(ZKClient.get().getChildren(path +  Constants.Algorithm.ZK_MODELSERVER_PATH).size()< totalModelServers){
						Thread.sleep(10);
					}
				} catch (InterruptedException e) {
					throw new FelucaException("waitForModelServerStarted  InterruptedException ",e );
				} catch (KeeperException e) {
					throw new FelucaException("waitForModelServerStarted  KeeperException ",e );
				}
			}
		}, 10000);

	}
	
	
	public void submitAndWait(Runnable runnable, long timeOutMills) throws InterruptedException, ExecutionException, TimeoutException{
		Future<?> submit = ConcurrentExecutor.submit(runnable);
		submit.get(timeOutMills, TimeUnit.MILLISECONDS);
	}
	
	public void setFinish() throws KeeperException, InterruptedException{
		ZKClient.get().setData(path, "finish".getBytes());
	}
	
	
	public String toString() {
		return path + " of (" + totalWorkers+ ")" ;
	}
	
}
