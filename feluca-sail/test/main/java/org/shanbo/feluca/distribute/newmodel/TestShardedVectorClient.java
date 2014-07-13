package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;

import java.net.UnknownHostException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;


public class TestShardedVectorClient {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		EventLoop loop = EventLoop.defaultEventLoop();

		Client cli = new Client("localhost", 1985, loop);
		VectorDB vector = cli.proxy(VectorDB.class);
		
		vector.createVector("test", 100, -1f, false);
		vector.dumpToDisk("test", null, 1, 0); 
		
		float[] multiGet = vector.multiGet("test", new int[]{1,2,4});
		System.out.println(TFloatArrayList.wrap(multiGet).toString());
		vector.multiUpdate("test", new int[]{1,2,4}, new float[]{4.0f, 4.3f,-2f});
		multiGet = vector.multiGet("test", new int[]{1,2,4});
		System.out.println(TFloatArrayList.wrap(multiGet).toString());
		vector.dumpToDisk("test", null, 1, 0); 
		
		cli.close();
		loop.shutdown();
		
		
		
	}

}
