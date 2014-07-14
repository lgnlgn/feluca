package org.shanbo.feluca.distribute.newmodel;

import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorServer extends Server{
	static Logger log = LoggerFactory.getLogger(VectorServer.class);
	EventLoop loop;
	org.msgpack.rpc.Server server;
	GlobalConfig conf;
	int port = 0;
	
	public VectorServer(GlobalConfig conf){
		this.conf = conf;
		port = Integer.parseInt(conf.getWorkerName().split(":")[1]) 
				+ Constants.Algorithm.ALGO_DATA_SERVER_PORTAWAY;
		
		
	}
	
	@Override
	public String serverName() {
		return "vectorServer";
	}

	@Override
	public int defaultPort() {
		return port;
	}

	@Override
	public String zkPathRegisterTo() {
		return Constants.Algorithm.ZK_ALGO_CHROOT + "/" + conf.getAlgorithmName() + Constants.Algorithm.ZK_MODELSERVER_PATH ;
	}

	@Override
	public void preStart() throws Exception {
		ClusterUtil.getWorkerList();
		loop = EventLoop.defaultEventLoop();
		server = new org.msgpack.rpc.Server(loop);
		server.serve(new VectorDBImpl());
		server.listen("0.0.0.0", defaultPort());
		System.out.println("modelServer started");
	}

	@Override
	public void postStop() throws Exception {
		server.close();
		loop.shutdown();
		System.out.println("modelServer closed");
	}
	
}
