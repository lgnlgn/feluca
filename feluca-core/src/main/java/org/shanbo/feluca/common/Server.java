package org.shanbo.feluca.common;

import java.net.SocketException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKUtil;
import org.shanbo.feluca.util.Config;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class Server {
	Logger log = LoggerFactory.getLogger(Server.class);
	protected String ip = null;
	protected int port;
	protected CuratorFramework zkClient ;
	public void start(){
		try{
			zkClient = ZKUtils.newClient();
			zkClient.start();
			this.preStart();
			if (zkClient.checkExists().forPath(zkPathRegisterTo()) == null ){
				zkClient.create().creatingParentsIfNeeded().forPath(zkPathRegisterTo());	
			}
			if (zkClient.checkExists().forPath(zkPathRegisterTo() + "/" + getServerAddress()) != null){
				zkClient.delete().guaranteed().forPath(zkPathRegisterTo() + "/" + getServerAddress());
			}
			zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(zkPathRegisterTo() + "/" + getServerAddress());
			
				//			ZKClient.get().createIfNotExist(zkPathRegisterTo());
//			ZKClient.get().registerEphemeralNode(zkPathRegisterTo(), getServerAddress());
		}catch (Exception e) {
			log.error("Server [" + this.getClass().getName() + "] start failed", e);
			throw new FelucaException("Server [" + this.getClass().getName() + "] start failed",e);
		}
		
	}

	public  void stop(){
		try{
			this.postStop();
			zkClient.delete().guaranteed().forPath(zkPathRegisterTo() + "/" +  getServerAddress());
//			ZKClient.get().unRegisterEphemeralNode(zkPathRegisterTo(), getServerAddress());
		}catch (Exception e) {
			log.error("Server [" + this.getClass().getName() + "] stop failed", e);
			throw new FelucaException("Server [" + this.getClass().getName() + "] stop failed");
		}finally{
			zkClient.close();
		}
	}

	public abstract String serverName();

	public abstract int defaultPort();
	
	public abstract String zkPathRegisterTo();
	
	public abstract void preStart() throws Exception;
	
	public abstract void postStop() throws Exception;
	
	
	public String getServerAddress(){
		if (ip == null){
			String myip = "0.0.0.0";
			try {
				myip = NetworkUtils.getFirstNonLoopbackAddress(
						NetworkUtils.StackType.IPv4).getHostAddress();
			} catch (SocketException e) {
			}
			Config c = Config.get();
			
			ip = c.get(this.serverName() + ".bind-address", myip);
			port = c.getInt(this.serverName() + ".bind-port", defaultPort());
		}
		return ip + ":" + port;
	}
}
