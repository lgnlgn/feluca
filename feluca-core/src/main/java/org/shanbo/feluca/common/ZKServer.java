package org.shanbo.feluca.common;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;


public class ZKServer implements Runnable, Closeable{
	Properties startupProperties ;
	ZooKeeperServerMain zooKeeperServer;
	public ZKServer(String path){
		
	}
	
	public ZKServer(){
		
	}
	
	public void run() {
		Properties startupProperties = new Properties();

		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(startupProperties);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}

		zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		try {
			zooKeeperServer.runFromConfig(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void close() throws IOException {
		
		
	}


}
