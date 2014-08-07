package org.shanbo.feluca.distribute.model.vertical;

import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.distribute.model.horizon.MModelRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * use for Reducer & MapReceiver
 * @author lgn
 *
 */
public class ReduceServer extends Server{
	static Logger log = LoggerFactory.getLogger(ReduceServer.class);
	EventLoop loop;
	org.msgpack.rpc.Server server;
	String algoName;
	int totalClients = 1;
	int port;
	public ReduceServer(String workerAddress, int totalClients,  String algoName){
		this.port = new Integer(workerAddress.split(":")[1]) + FloatReducer.PORT_AWAY;
		this.algoName = algoName;
		this.totalClients = totalClients;
	}
	
	@Override
	public String serverName() {
		return "reduceServer";
	}

	@Override
	public int defaultPort() {
		return port;
	}

	@Override
	public String zkPathRegisterTo() {
		return Constants.Algorithm.ZK_ALGO_CHROOT + "/" + algoName + "/reducer" ;

	}

	@Override
	public void preStart() throws Exception {
//		ClusterUtil.getWorkerList();
		loop = EventLoop.defaultEventLoop();
		server = new org.msgpack.rpc.Server(loop);
		server.serve(new FloatReducerImpl(totalClients));
		server.listen("0.0.0.0", defaultPort());
		System.out.println("reduceServer[" + port + "] started");

		
	}

	public String getServerAddress(){
		return super.getServerAddress().split(":")[0] + ":" +  port;
	}
	
	
	@Override
	public void postStop() throws Exception {
		server.close();
		loop.shutdown();
		System.out.println("reduceServer[" + port + "] closed");
		
	}

}
