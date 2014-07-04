package org.shanbo.feluca.distribute.launch;


import java.net.SocketException;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.StringValueWatcher;

public class LoopMonitor {

	private boolean loopOk = false;
	private StringValueWatcher loopWatcher;
	private String path ;
	public LoopMonitor(String taskName){
		this.path = Constants.Algorithm.ZK_ALGO_CHROOT + "/" + taskName;
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
		while(!loopOk){
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		loopOk = false;
	}
	
	public void confirmLoopFinish() throws Exception{
		ZKClient.get().createIfNotExist(path + Constants.Algorithm.ZK_WAITING_PATH + "/" + NetworkUtils.ipv4Host());
	}
	
	public void close(){
		ZKClient.get().destoryWatch(loopWatcher);
	}
	
}
