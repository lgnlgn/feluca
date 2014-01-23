package org.shanbo.feluca.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;

/**
 *  in order to finish initialization(<b>the watcher is in a  asynchronous way</b>) before usage, 
 *  invoke this when server started;
 * 
 *	@author shanbo.liang
 */
public class ClusterUtil {
	private Map<String, String> workerAddresses;
	private static ClusterUtil instance = new ClusterUtil();
	
	private ClusterUtil(){
		workerAddresses = new ConcurrentHashMap<String, String>();
		ZKClient.get().watchChildren(Constants.Base.ZK_WORKER_PATH, new ChildrenWatcher() {
			@Override
			public void nodeRemoved(String node) {
				removeSlave(node);
			}
			@Override
			public void nodeAdded(String node) {
				addSlave(node);
			}
		});
	}
	
	private void addSlave(String hostPort){
		this.workerAddresses.put(hostPort, "");
	}
	
	private String removeSlave(String hostPort){
		return this.workerAddresses.remove(hostPort);
	}
	
	public static List<String> getWorkerList(){
		List<String> result = new ArrayList<String>();
		result.addAll(instance.workerAddresses.keySet());
		return result;
	}
	
	public static String getLeaderAddress() throws InterruptedException, KeeperException{
		List<String> leader = ZKClient.get().getChildren(Constants.Base.LEADER_REPOSITORY);
		return leader.isEmpty() ? null : leader.get(0);
	}
	
	public static String getFDFSAddress() throws InterruptedException, KeeperException{
		List<String> leader = ZKClient.get().getChildren(Constants.Base.FDFS_ZK_ROOT);
		return leader.isEmpty() ? null : leader.get(0);
	}
}
