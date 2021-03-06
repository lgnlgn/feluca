package org.shanbo.feluca.distribute.launch;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.StringValueWatcher;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class LoopMonitor {
	
	static Logger log = LoggerFactory.getLogger(LoopMonitor.class);
	
	private boolean loopOk = false;
	private StringValueWatcher loopWatcher;
	private final String path ;
	private String workerName;
	public LoopMonitor(String taskName, String workerName){
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.workerName = workerName;
	}

	/**
	 * Waits for '/workers' to be created by StartingGun; then watch the '/loop' node
	 * If the '/workers' is not created , calling of the {@link #confirmLoopFinish()} will, in process view, prematurely registers the ready-signal !
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	public void start() throws KeeperException, InterruptedException, ExecutionException, TimeoutException{
		ZKClient.get().createIfNotExist(path);
		waitForSignalEquals(StartingGun.START_SIGNAL, 100000);
		loopWatcher = new StringValueWatcher() {
			public void valueChanged(String l) {
				loopOk = true;
			}
		};
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_LOOP_PATH);
		ZKClient.get().watchStrValueNode(path + Constants.Algorithm.ZK_LOOP_PATH, loopWatcher);
	}

	public void waitForLoopStart(){
		while(!loopOk){ //until watched value changed 
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				log.warn("InterruptedException in waitForLoopStart");
			}
		}
		loopOk = false;
	}

	/**
	 * 
	 * @param signal
	 * @param timeOutMills
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws TimeoutException
	 * @throws ExecutionException
	 */
	private void waitForSignalEquals(final String signal, long timeOutMills) throws KeeperException, InterruptedException, TimeoutException, ExecutionException{
		ConcurrentExecutor.submitAndWait(new Runnable() {
			public void run() {
				while(true){
					try {
						if (!ZKClient.get().getStringData(path).equals(signal)){
							Thread.sleep(10);
						}else{
							return ;
						}
					} catch (KeeperException e) {
						break;
					} catch (InterruptedException e) {
						break;
					}		
				}
				throw new FelucaException("break from waitForSignalEquals");
			}
		}, timeOutMills);
	}

	public void waitForFinish() throws KeeperException, InterruptedException, TimeoutException, ExecutionException{
		this.waitForSignalEquals(StartingGun.FINISH_SIGNAL, 100000);
	}

	public void confirmLoopFinish() throws Exception{
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_WAITING_PATH  + "/" +  workerName);
	}

	public void close(){
		ZKClient.get().destoryWatch(loopWatcher);
		System.out.println("loopMonitor of [" + workerName + "] closed");
	}

	public String toString(){
		return ".../" + path + "/" + workerName; 
	}

}
