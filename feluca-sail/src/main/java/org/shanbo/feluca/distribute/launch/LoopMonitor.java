package org.shanbo.feluca.distribute.launch;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
		String workingNode = path + Constants.Algorithm.ZK_WAITING_PATH + "/" + workerName;
		ZKClient.get().createIfNotExist(workingNode);
		ZKClient.get().getZooKeeper().create(workingNode, new byte[]{0x1}, ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public void close(){
		ZKClient.get().destoryWatch(loopWatcher);
		System.out.println("loopMonitor of [" + workerName + "] closed");
	}
	

	
}
