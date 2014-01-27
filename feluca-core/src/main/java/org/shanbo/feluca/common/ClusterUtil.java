package org.shanbo.feluca.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.shanbo.feluca.node.leader.LeaderServer;
import org.shanbo.feluca.node.worker.WorkerServer;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  in order to finish initialization(<b>the watcher is in a  asynchronous way</b>) before usage, 
 *  invoke this when server started;
 * 
 *	@author shanbo.liang
 */
public class ClusterUtil {
	final static Logger log = LoggerFactory.getLogger(ClusterUtil.class);
	private Map<String, String> workerAddresses;
	private Properties defaultProp ;
	private static ClusterUtil instance = new ClusterUtil();
	
	private ClusterUtil(){
		workerAddresses = new ConcurrentHashMap<String, String>();
		defaultProp = new Properties();
		defaultProp.put("leader.repo", "./leader_repo");
		defaultProp.put("worker.repo", "./worker_repo");
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
		try {
			String feluca = ZKClient.get().getStringData(Constants.Base.ZK_CHROOT);
			if (feluca == null){
				ZKClient.get().createIfNotExist(Constants.Base.ZK_CHROOT);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				defaultProp.store(bos, "");
				ZKClient.get().setData(Constants.Base.ZK_CHROOT, bos.toByteArray());
			}else{
				defaultProp.load(new ByteArrayInputStream(feluca.getBytes()));
			}
		} catch (Exception e) {
			log.error("init failed ",e);
		} 
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
		List<String> leader = ZKClient.get().getChildren(Constants.Base.getLeaderRepository());
		return leader.isEmpty() ? null : leader.get(0);
	}
	
	public static String getFDFSAddress() throws InterruptedException, KeeperException{
		List<String> leader = ZKClient.get().getChildren(Constants.Base.FDFS_ZK_ROOT);
		return leader.isEmpty() ? null : leader.get(0);
	}
	
	public static String getProperties(String key, String defaultValue){
		return instance.defaultProp.getProperty(key, defaultValue);
	}
}
