package org.shanbo.feluca.distribute.newmodel;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

/**
 * manage the sharding strategy here; i.e. server side is just a vector collection; 
 * @author lgn
 *
 */
public class VectorClient {
	
	
	final static int MAX_TMP_FIDS_PER_SHARD = 200000;
	
	EventLoop loop;
	Map<String,PartialVectorModel> currentVectorsMap;
	
	//buffers for partition fids to different shards
	Map<String, TIntArrayList[]> tmpConvertedFidsMap;
	Map<String, TFloatArrayList[]> tmpToUpdateValuesMap;
	
	
	Client[] clients;
	VectorDB[] vectorDBs;
	List<String> dataServerAddresses;

	FidPartitioner partitioner;

	Map<String, Integer> globalVectorSizeMap ;

	/**
	 * list order must be the same with the input for server 
	 * @param vectorServers
	 * @throws UnknownHostException 
	 * @throws NumberFormatException 
	 */
	public VectorClient(GlobalConfig globalConfig) throws NumberFormatException, UnknownHostException{
		loop = EventLoop.defaultEventLoop();
		
		globalVectorSizeMap = new HashMap<String, Integer>(3);
		currentVectorsMap = new HashMap<String, PartialVectorModel>(3);
		tmpConvertedFidsMap = new HashMap<String, TIntArrayList[]>(3);
		tmpToUpdateValuesMap = new HashMap<String, TFloatArrayList[]>(3);
		clients = new Client[globalConfig.getModelServers().size()];
		vectorDBs = new VectorDB[globalConfig.getModelServers().size()];
		dataServerAddresses =  globalConfig.getModelServers();
		
		partitioner = new FidPartitioner.HashPartitioner(dataServerAddresses.size());
	}

	public void open() throws NumberFormatException, UnknownHostException{
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = dataServerAddresses.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]), loop);
			vectorDBs[i] = clients[i].proxy(VectorDB.class);
		}
	}
	
	public void createPartialModel(String vectorName){
		if (!tmpConvertedFidsMap.containsKey(vectorName)){
			TIntArrayList[] tmpConvertedFidsList = new TIntArrayList[clients.length];
			tmpConvertedFidsMap.put(vectorName, tmpConvertedFidsList);
			TFloatArrayList[] tmpToUpdateValuesList = new TFloatArrayList[clients.length];
			tmpToUpdateValuesMap.put(vectorName, tmpToUpdateValuesList);
			for(int i = 0 ; i < tmpConvertedFidsList.length; i++){
				tmpConvertedFidsList[i] = new TIntArrayList(1000);
				tmpToUpdateValuesList[i]= new TFloatArrayList(1000);
			}
			currentVectorsMap.put(vectorName, new PartialVectorModel());
		}
	}
	
	public synchronized void createVector(final String vectorName, int globalVectorSize, final float defaultValue) throws InterruptedException, ExecutionException{
//		this.globalVectorSizeMap.put(vectorName, globalVectorSize);
//		if (!tmpConvertedFidsMap.containsKey(vectorName)){
//			TIntArrayList[] tmpConvertedFidsList = new TIntArrayList[clients.length];
//			tmpConvertedFidsMap.put(vectorName, tmpConvertedFidsList);
//			TFloatArrayList[] tmpToUpdateValuesList = new TFloatArrayList[clients.length];
//			tmpToUpdateValuesMap.put(vectorName, tmpToUpdateValuesList);
//			for(int i = 0 ; i < tmpConvertedFidsList.length; i++){
//				tmpConvertedFidsList[i] = new TIntArrayList(1000);
//				tmpToUpdateValuesList[i]= new TFloatArrayList(1000);
//			}
//			currentVectorsMap.put(vectorName, new PartialVectorModel());
//		}
		
		
		final int perVectorSize = globalVectorSize / clients.length + 2;
		ArrayList<Callable<Void>> createCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){

			final int toShardId = shardId;
			createCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     vectorDBs[toShardId].createVector(vectorName, perVectorSize, defaultValue, false);
				     return null ;
				}
			});

		}
		ConcurrentExecutor.execute(createCallables);
	}
	
	public synchronized  void dumpVector(final String vectorName, final String path) throws InterruptedException, ExecutionException{
		ArrayList<Callable<Void>> createCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){

			final int toShardId = shardId;
			createCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     vectorDBs[toShardId].dumpToDisk(vectorName, path, clients.length, toShardId);
				     return null ;
				}
			});

		}
		ConcurrentExecutor.execute(createCallables);
	}
	
	

	public synchronized void fetchVector(final String vectorName, int[] fids) throws InterruptedException, ExecutionException{
		TIntArrayList[] tmpConvertedFidsList = tmpConvertedFidsMap.get(vectorName);
		TFloatArrayList[] tmpToUpdateValuesList = tmpToUpdateValuesMap.get(vectorName);
		if (tmpConvertedFidsList == null)
			throw new FelucaException("not found : '" + vectorName + "'! you need create it first");

		//----clear the fid[] buffer
		for(int i = 0 ; i  < tmpConvertedFidsList.length; i++){
			if (tmpConvertedFidsList[i].size() > MAX_TMP_FIDS_PER_SHARD){
				tmpConvertedFidsList[i].clear(1000);
				tmpToUpdateValuesList[i].clear(1000);
			}else{
				tmpConvertedFidsList[i].resetQuick();
				tmpToUpdateValuesList[i].resetQuick();
			}
		}
		//----partition to different shards
		for(int fid : fids){
			int shardId = partitioner.decideShard(fid);
			tmpConvertedFidsList[shardId].add(partitioner.featureIdToIndex(fid, shardId));
		}
		
		//concurrent gets
		ArrayList<Callable<float[]>> mGetCallables = new ArrayList<Callable<float[]>>(tmpConvertedFidsList.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = tmpConvertedFidsList[shardId].toArray();
			
			final int toShardId = shardId;
			mGetCallables.add(new Callable<float[]>() {
				public float[] call() throws Exception {
					return vectorDBs[toShardId].multiGet(vectorName, toShardFids);	
				}
			});

		}
		//fetched  result; merge
		List<float[]> mGetValues = ConcurrentExecutor.execute(mGetCallables);
		PartialVectorModel currentVector = currentVectorsMap.get(vectorName);
		currentVector.checkAndCompact();
		for(int shardId = 0 ; shardId < tmpConvertedFidsList.length; shardId++){
			TIntArrayList tmpConvertedFids = tmpConvertedFidsList[shardId];
			float[] mGetThisShard = mGetValues.get(shardId);
			for(int fi = 0 ; fi < tmpConvertedFids.size(); fi++){
				int convertedFid = tmpConvertedFids.getQuick(fi);
				int originalFid = partitioner.indexToFeatureId(convertedFid, shardId);
				float value = mGetThisShard[fi];
				currentVector.setForMerge(originalFid, value);
			}
		}
	}

	/**
	 * be careful. call it after {@link #fetchVector(String, int[])}
	 * @param vectorName
	 * @param fids
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public synchronized void updateCurrentVector(final String vectorName, final  int[] fids) throws InterruptedException, ExecutionException{
		TFloatArrayList[] tmpToUpdateValuesList = tmpToUpdateValuesMap.get(vectorName);
		TIntArrayList[] tmpConvertedFidsList = tmpConvertedFidsMap.get(vectorName);
		PartialVectorModel model = currentVectorsMap.get(vectorName);
		model.splitValuesByFIds(fids, partitioner, tmpToUpdateValuesList);
		//
		ArrayList<Callable<Void>> mUpdateCallables = new ArrayList<Callable<Void>>(tmpToUpdateValuesList.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = tmpConvertedFidsList[shardId].toArray();
			final float[] toShardValues = tmpToUpdateValuesList[shardId].toArray();
			final int toShardId = shardId;
			mUpdateCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
					vectorDBs[toShardId].multiUpdate(vectorName, toShardFids , toShardValues);	
					return null;
				}
			});

		}
		ConcurrentExecutor.execute(mUpdateCallables);
	}
	
	
	
	
	public synchronized void close(){
		for(Client client : clients){
			client.close();
		}
		loop.shutdown();
		System.out.println("vectorClients of #" + clients.length + " all closed");
	}
	
	public PartialVectorModel getVector(String vectorName){
		return currentVectorsMap.get(vectorName);
	}
	
}
