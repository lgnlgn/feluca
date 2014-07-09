package org.shanbo.feluca.distribute.newmodel;

import java.io.IOException;

import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;


public class TestShardedVectorServer {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		EventLoop loop = EventLoop.defaultEventLoop();

		Server svr = new Server();
		
		svr.serve(new VectorDBImpl());
		svr.listen(1985);
		System.out.println("!1");
	}

}
