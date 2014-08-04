package org.shanbo.feluca.distribute.launch;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

	public final static String START_SIGNAL = "start";
	public final static String FINISH_SIGNAL = "finish";
	
	
	private ChildrenWatcher workerWatcher;
	
	private int totalWorkers; 
	private int totalReduceServers;
	private AtomicInteger waitingWorkers;
	private  String path;
	private int loop;
	
	public StartingGun(String taskName, int totalReduceServers, int totalWorkers) {
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.totalWorkers = totalWorkers;
		this.totalReduceServers = totalReduceServers;
		waitingWorkers = new AtomicInteger(0);
	}
	
	private void prepare() throws KeeperException, InterruptedException{
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_LOOP_PATH); 
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_WAITING_PATH);
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_REDUCER_PATH);
		//DO NOT delete '/worker' and recreate it! It will cause a bug that the ZKclient will not be able to create children  
		// Instead, delete it's children 
		List<String> waitingList = ZKClient.get().getChildren(path + Constants.Algorithm.ZK_WAITING_PATH);
		for(String workerNode : waitingList){
			ZKClient.get().forceDelete(path + Constants.Algorithm.ZK_WAITING_PATH + "/" + workerNode);
		}
		ZKClient.get().setData(path, START_SIGNAL.getBytes());
	}
	
	private void startWatch() {
		workerWatcher = new ChildrenWatcher() {
			public void nodeRemoved(String node) {
			}
			public void nodeAdded(String node) {
				waitingWorkers.incrementAndGet();
				if (waitingWorkers.get()  == totalWorkers){
					synchronized(StartingGun.class){ //Double check lock 
						if (waitingWorkers.get() == totalWorkers)
							setLoopSignal();
					}
				}
			}
		};
		ZKClient.get().watchChildren(path + Constants.Algorithm.ZK_WAITING_PATH, workerWatcher);
	}
	
	private void setLoopSignal(){
		waitingWorkers.set(0);
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
		this.prepare();
		this.startWatch();
	}
	
	public void close() throws InterruptedException, KeeperException{
		ZKClient.get().destoryWatch(workerWatcher);
		ZKClient.get().forceDelete(path + Constants.Algorithm.ZK_LOOP_PATH);
		ZKClient.get().forceDelete(path + Constants.Algorithm.ZK_WAITING_PATH);
		System.out.println("startingGun of [" + path + "] closed"  );
	}
	
	
	public void waitForModelServersStarted() throws InterruptedException, ExecutionException, TimeoutException{
		ConcurrentExecutor.submitAndWait(new Runnable() {
			public void run() {
				try{
					while(ZKClient.get().getChildren(path +  Constants.Algorithm.ZK_REDUCER_PATH).size()< totalReduceServers
						&& ZKClient.get().getChildren(path +  Constants.Algorithm.ZK_MODELSERVER_PATH).size() < totalWorkers){
						try{
							Thread.sleep(10);
						}catch (InterruptedException e) {
							break;
						}
					}
				} catch (InterruptedException e) {
					throw new FelucaException("waitForModelServerStarted  InterruptedException ",e );
				} catch (KeeperException e) {
					throw new FelucaException("waitForModelServerStarted  KeeperException ",e );
				}
			}
		}, 10000);

	}

	
	public void setFinish() throws KeeperException, InterruptedException{
		ZKClient.get().setData(path, FINISH_SIGNAL.getBytes());
	}
	
	
	public String toString() {
		return path + " of (" + totalWorkers+ ")" ;
	}
	
}
