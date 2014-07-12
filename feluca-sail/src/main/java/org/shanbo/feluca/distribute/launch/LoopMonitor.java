package org.shanbo.feluca.distribute.launch;


import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.StringValueWatcher;

public class LoopMonitor {

	private boolean loopOk = false;
	private StringValueWatcher loopWatcher;
	private String path ;
	private String workerName;
	public LoopMonitor(String taskName, String workerName){
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
		this.workerName = workerName;
		watchLoopSignal();
	}
	
	private void watchLoopSignal(){
		loopWatcher = new StringValueWatcher() {
			public void valueChanged(String l) {
				loopOk = true;
			}
		};
		ZKClient.get().watchStrValueNode(path + Constants.Algorithm.ZK_LOOP_PATH, loopWatcher);
	}
	
	public void waitForLoopStart(){
		while(!loopOk){ //until watched value changed 
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		loopOk = false;
	}
	
	public void waitForSignalEquals(String signal, long timeOutMills) throws KeeperException, InterruptedException{
		long t = System.currentTimeMillis();
		while(true){
			long t2 = System.currentTimeMillis() - t;
			if (t2 > timeOutMills){
				throw new InterruptedException("waitForSignalEquals   time out!");
			}
			if (ZKClient.get().getStringData(path).equals(signal)){
				break;
			}else{
				Thread.sleep(10);
			}
		}
	}
	
	
	public void confirmLoopFinish() throws Exception{
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_WAITING_PATH + "/" + workerName);
	}
	
	public void close(){
		
		ZKClient.get().destoryWatch(loopWatcher);
	}
	

	
}
