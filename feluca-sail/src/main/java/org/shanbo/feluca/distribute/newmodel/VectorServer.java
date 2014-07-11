package org.shanbo.feluca.distribute.newmodel;

import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.util.NetworkUtils;

public class VectorServer extends Server{
	EventLoop loop;
	org.msgpack.rpc.Server server;
	GlobalConfig conf;
	String host ;
	int port = 0;
	
	public VectorServer(GlobalConfig conf, String workerName){
		this.conf = conf;
		host = workerName.split(":")[0];
		port = Integer.parseInt(workerName.split(":")[1]) + 100;
		
		
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
	public String zkRegisterPath() {
		return Constants.Algorithm.ZK_ALGO_CHROOT + "/" + conf.getAlgorithmName() + "/model" ;
	}

	@Override
	public void preStart() throws Exception {
		ClusterUtil.getWorkerList();
		loop = EventLoop.defaultEventLoop();
		server = new org.msgpack.rpc.Server(loop);
		server.serve(new VectorDBImpl());
		server.listen(NetworkUtils.ipv4Host(), defaultPort());
	}

	@Override
	public void postStop() throws Exception {
		server.close();
		loop.shutdown();
		
	}
	
}
