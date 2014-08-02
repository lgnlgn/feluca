package org.shanbo.feluca.distribute.model.vertical;

import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
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
	int port = 0;
	FloatReducerImpl reducer;
	String algoName;
	public ReduceServer(FloatReducerImpl reducer, String algoName, int port){
		this.reducer = reducer;
		this.port = port;
		this.algoName = algoName;
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
		return Constants.Algorithm.ZK_ALGO_CHROOT + "/" + algoName + Constants.Algorithm.ZK_MODELSERVER_PATH ;

	}

	@Override
	public void preStart() throws Exception {
		ClusterUtil.getWorkerList();
		loop = EventLoop.defaultEventLoop();
		server = new org.msgpack.rpc.Server(loop);
		server.serve(reducer);
		server.listen("0.0.0.0", defaultPort());
		System.out.println("reduceServer[" + port + "] started");

		
	}

	@Override
	public void postStop() throws Exception {
		server.close();
		loop.shutdown();
		System.out.println("reduceServer[" + port + "] closed");
		
	}

}
