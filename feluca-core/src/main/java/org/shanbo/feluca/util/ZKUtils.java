package org.shanbo.feluca.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKUtils {
	public static CuratorFramework newClient(){
		return CuratorFrameworkFactory.newClient(Config.get().get("zk.quorum"), 
				Config.get().getInt("zk.session.timeout", 3000), 
				1000,  
				new ExponentialBackoffRetry(1000, 3));

	}
	
	public  static void createIfNotExist(CuratorFramework zk, String fullPath) throws Exception{
		if (zk.checkExists().forPath(fullPath) == null){
			zk.create().creatingParentsIfNeeded().inBackground().forPath(fullPath, new byte[]{});
		}
	}
}
