package org.shanbo.feluca.distribute.newmodel;


import gnu.trove.list.array.TFloatArrayList;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.model.HashPartitioner;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class ReduceClient implements Closeable{
	EventLoop loop;
	Client[] clients;
	FloatReducer[] reducers; //to workers back
	final int clientId;
	HashPartitioner partitioner;
	
	List<String> reduceServerAddresses ;
	ArrayList<TFloatArrayList> container; 
	
	public ReduceClient(GlobalConfig conf){
		this.clientId = conf.getWorkers().indexOf(conf.getWorkerName());
		this.loop = EventLoop.defaultEventLoop();
		this.reduceServerAddresses = conf.getModelServers();
		clients = new Client[conf.getModelServers().size()];
		reducers = new FloatReducer[conf.getModelServers().size()];
		partitioner = new HashPartitioner(conf.getModelServers().size());
		container = new ArrayList<TFloatArrayList>(clients.length);
		for(int i = 0 ; i < clients.length; i++){
			container.add(new TFloatArrayList());
		}
	}
	
	public void open() throws NumberFormatException, UnknownHostException{
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = reduceServerAddresses.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]), loop);
			reducers[i] = clients[i].proxy(FloatReducer.class);
		}
	}
	
	private void clearContainer(){
		for(TFloatArrayList cont : container){
			cont.resetQuick();
		}
	}
	
	public float[] fetch(float[] computedValues) throws InterruptedException, ExecutionException{
		clearContainer();
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
				     return reducers[toShardId].reduce(clientId, values);
				     
				}
			});
		}
		List<float[]> results = ConcurrentExecutor.execute(reduceCalls);
		float[] reduceBacks = new float[computedValues.length];
		for(int shardId = 0; shardId < results.size(); shardId++){
			for(int idx = 0 ; idx < results.get(shardId).length ; idx ++){
				int fid = partitioner.indexToFeatureId(idx, shardId);
				reduceBacks[fid] = results.get(shardId)[idx];
			}
		}
		return reduceBacks;
	}
	
	public void close(){
		for(Client client : clients){
			client.close();
		}
		loop.shutdown();
		System.out.println("reduceClients of #" + clients.length + " all closed");

	}
}
