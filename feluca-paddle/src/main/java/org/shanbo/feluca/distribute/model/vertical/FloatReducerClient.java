package org.shanbo.feluca.distribute.model.vertical;


import gnu.trove.list.array.TFloatArrayList;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class FloatReducerClient implements Closeable{
	EventLoop loop;
	Client[] clients;
	FloatReducer[] reducers; //to workers back
	final int clientId;
	HashPartitioner partitioner;

	List<String> reduceServer ;
	ArrayList<TFloatArrayList> container; //do parallel computing on more reducer maybe faster

	public FloatReducerClient(List<String> reducerServer, int shardId){
		this.clientId = shardId;
		this.reduceServer = reducerServer;
		clients = new Client[reducerServer.size()];
		reducers = new FloatReducer[reducerServer.size()];
		partitioner = new HashPartitioner(reducerServer.size());
		container = new ArrayList<TFloatArrayList>(clients.length);
		for(int i = 0 ; i < clients.length; i++){
			container.add(new TFloatArrayList());
		}
	}

	public void connect() throws NumberFormatException, UnknownHostException{
		this.loop = EventLoop.defaultEventLoop();
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = reduceServer.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]) + FloatReducer.PORT_AWAY, loop);
			reducers[i] = clients[i].proxy(FloatReducer.class);
		}
	}


	public float[] sum(float[] computedValues) throws InterruptedException, ExecutionException{
		return reduce("sum", computedValues);
	}

	public float[] max(float[] computedValues) throws InterruptedException, ExecutionException{
		return reduce("max", computedValues);
	}

	public float[] min(float[] computedValues) throws InterruptedException, ExecutionException{
		return reduce("min", computedValues);
	}

	public float[] avg(float[] computedValues) throws InterruptedException, ExecutionException{
		return reduce("avg", computedValues);
	}


	private float[] reduce(final String func, float[] computedValues) throws InterruptedException, ExecutionException{
		for(TFloatArrayList cont : container){
			cont.resetQuick();
		}
		for(int i = 0 ; i < computedValues.length; i++){
			int reducerIndex = partitioner.decideShard(i);
			container.get(reducerIndex).add(computedValues[i]);
		}
		ArrayList<Callable<float[]>> reduceCalls = new ArrayList<Callable<float[]>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int toShardId = shardId;
			final float[] values = container.get(shardId).toArray();
			reduceCalls.add(new Callable<float[]>() {
				public float[] call() throws Exception {
					return reducers[toShardId].reduce(func, clientId, values);

				}
			});
		}
		List<float[]> results = ConcurrentExecutor.execute(reduceCalls);
		float[] reduceBacks = new float[computedValues.length];
		for(int shardId = 0; shardId < results.size(); shardId++){
			for(int idx = 0 ; idx < results.get(shardId).length ; idx ++){
				int fid = partitioner.indexToId(idx, shardId);
				reduceBacks[fid] = results.get(shardId)[idx];
			}
		}
		return reduceBacks;
	}

	public void close(){
		if (loop != null){
			for(Client client : clients){
				client.close();
			}
			loop.shutdown();
			System.out.println("reduceClients of #" + clients.length + " all closed");
		}
	}
}
