package org.shanbo.feluca.common;

import java.net.SocketException;

import org.shanbo.feluca.util.Config;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.ZKClient;
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

	public void start(){
		try{
			this.preStart();
			ZKClient.get().registerEphemeralNode(zkRegisterPath(), getServerAddress());
		}catch (Exception e) {
			log.error("Server [" + this.getClass().getName() + "] start failed", e);
			throw new FelucaException("Server [" + this.getClass().getName() + "] start failed");
		}
		
	}

	public  void stop(){
		try{
			this.postStop();
			ZKClient.get().unRegisterEphemeralNode(zkRegisterPath(), getServerAddress());
		}catch (Exception e) {
			log.error("Server [" + this.getClass().getName() + "] stop failed", e);
			throw new FelucaException("Server [" + this.getClass().getName() + "] stop failed");
		}
	}

	public abstract String serverName();

	public abstract int defaultPort();
	
	public abstract String zkRegisterPath();
	
	public abstract void preStart() throws Exception;
	
	public abstract void postStop() throws Exception;
	
	
	public String getServerAddress(){
		if (ip != null){
			String myip = "0.0.0.0";
			try {
				myip = NetworkUtils.getFirstNonLoopbackAddress(
						NetworkUtils.StackType.IPv4).getHostAddress();
			} catch (SocketException e) {
			}
			Config c = Config.get();
			if (c.get(this.serverName() + ".bind-address") == null || c.get(this.serverName() + ".bind-port") == null){
				System.out.println("YOURSERVER.bind-address OR port not set!!!! use default value");
			}
			ip = c.get(this.serverName() + ".bind-address", myip);
			port = c.getInt(this.serverName() + ".bind-port", defaultPort());
		}
		return ip + ":" + port;
	}
}
