package org.shanbo.feluca.distribute.model;

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

/**
 * <p>matrix still need testing;
 * <p>To use it: follow the 'fetch-then-update' way;
 * <p>needs delete method
 * @author lgn
 *
 */
public class ModelClient {

	EventLoop loop;
	Map<String,PartialVectorModel> currentVectorsMap;
	Map<String,PartialMatrixModel> currentMatrixesMap;
	
	Client[] clients;
	MatrixModel[] matrixModels;
	List<String> dataServerAddresses;
	
	HashPartitioner partitioner;
	
	public ModelClient(GlobalConfig globalConfig){
		loop = EventLoop.defaultEventLoop();
		currentVectorsMap = new HashMap<String, PartialVectorModel>(3);
		currentMatrixesMap = new HashMap<String, PartialMatrixModel>(3);
		
		clients = new Client[globalConfig.getModelServers().size()];
		matrixModels = new MatrixModel[globalConfig.getModelServers().size()];
		dataServerAddresses =  globalConfig.getModelServers();
		
		partitioner = new HashPartitioner(dataServerAddresses.size());
	}
	
	public PartialVectorModel getVector(String vectorName){
		return currentVectorsMap.get(vectorName);
	}
	
	public PartialMatrixModel getMatrix(String matrixName){
		return currentMatrixesMap.get(matrixName);
	}
	
	public void open() throws NumberFormatException, UnknownHostException{
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = dataServerAddresses.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]), loop);
			matrixModels[i] = clients[i].proxy(MatrixModel.class);
		}
	}
	public synchronized void close(){
		for(Client client : clients){
			client.close();
		}
		loop.shutdown();
		System.out.println("modelClients of #" + clients.length + " all closed");
	}
	public void createVector(final String vectorName, int globalVectorSize, final float defaultValue) throws InterruptedException, ExecutionException{
		currentVectorsMap.put(vectorName, new PartialVectorModel(partitioner));
		final int perVectorSize = globalVectorSize / clients.length + 2;
		ArrayList<Callable<Void>> createCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int toShardId = shardId;
			createCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     matrixModels[toShardId].vectorCreate(vectorName, perVectorSize, defaultValue, true);
				     return null ;
				}
			});
		}
		ConcurrentExecutor.execute(createCallables);
	}
	

	public PartialVectorModel vectorRetrieve(final String vectorName, int[] fids) throws InterruptedException, ExecutionException{
		PartialVectorModel vectorModel = getVector(vectorName);
		if (vectorModel == null){
			throw new FelucaException("vector not found : '" + vectorName + "'! you need create it first");
		}
		vectorModel.checkTmpBufferList();
		//split fids[] into different-shard indexes;
		int[][] splittedIndexIds = vectorModel.splitFids(fids);
		
		//prepare parallel retrieval
		ArrayList<Callable<float[]>> mGetCallables = new ArrayList<Callable<float[]>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = splittedIndexIds[shardId];
			
			final int toShardId = shardId;
			mGetCallables.add(new Callable<float[]>() {
				public float[] call() throws Exception {
					return matrixModels[toShardId].vectorRetrieve(vectorName, toShardFids);	
				}
			});
		}
		List<float[]> mGetValues = ConcurrentExecutor.execute(mGetCallables);
		//merge
		vectorModel.mergeValuesList(mGetValues);
		//
		return vectorModel;
	}
	
	public void vectorUpdate(final String vectorName, final  int[] fids) throws InterruptedException, ExecutionException{
		PartialVectorModel vectorModel = getVector(vectorName);
		if (vectorModel == null){
			throw new FelucaException("vector not found : '" + vectorName + "'! you need create it first");
		}
		//split fids[] into different-shard indexes(already do in retrieval);
		//split values[] then
		int[][] splittedIndexIds = vectorModel.splitFidsQuick();
		float[][] splittedValues = vectorModel.splitValues(fids);
		
		//prepare parallel update
		ArrayList<Callable<Void>> mUpdateCallables = new ArrayList<Callable<Void>>( clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = splittedIndexIds[shardId];
			final float[] toShardValues = splittedValues[shardId];
			final int toShardId = shardId;
			mUpdateCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
					matrixModels[toShardId].vectorUpdate(vectorName, toShardFids , toShardValues);	
					return null;
				}
			});

		}
		ConcurrentExecutor.execute(mUpdateCallables);
		
	}
	
	public synchronized  void dumpVector(final String vectorName, final String path) throws InterruptedException, ExecutionException{
		ArrayList<Callable<Void>> dumpCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int toShardId = shardId;
			dumpCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     matrixModels[toShardId].vectorDump(vectorName, path, clients.length, toShardId);
				     return null ;
				}
			});
		}
		ConcurrentExecutor.execute(dumpCallables);
	}

	
	public void createMatrix(final String matrixName, int globalRowSize, final int columnSize, final float defaultValue) throws InterruptedException, ExecutionException{
		currentMatrixesMap.put(matrixName, new PartialMatrixModel(partitioner));
		final int perRowSize = globalRowSize / clients.length + 2;
		ArrayList<Callable<Void>> createCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int toShardId = shardId;
			createCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     matrixModels[toShardId].matrixCreate(matrixName, perRowSize, columnSize, defaultValue, true);
				     return null ;
				}
			});
		}
		ConcurrentExecutor.execute(createCallables);
	}
	
	
	public PartialMatrixModel matrixRetrieve(final String matrixName, int[] fids) throws InterruptedException, ExecutionException{
		PartialMatrixModel matrixModel = getMatrix(matrixName);
		if (matrixModel == null){
			throw new FelucaException("vector not found : '" + matrixName + "'! you need create it first");
		}
		matrixModel.checkTmpBufferList();
		//split fids[] into different-shard indexes;
		int[][] splittedIndexIds = matrixModel.splitFids(fids);
		
		//prepare parallel retrieval
		ArrayList<Callable<float[][]>> mGetCallables = new ArrayList<Callable<float[][]>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = splittedIndexIds[shardId];
			
			final int toShardId = shardId;
			mGetCallables.add(new Callable<float[][]>() {
				public float[][] call() throws Exception {
					return matrixModels[toShardId].matrixRetrieve(matrixName, toShardFids);	
				}
			});
		}
		List<float[][]> mGetValues = ConcurrentExecutor.execute(mGetCallables);
		//merge
		matrixModel.mergeValuesList(mGetValues);
		//
		return matrixModel;
	}
	
	public void matrixUpdate(final String matrixName, final  int[] fids) throws InterruptedException, ExecutionException{
		PartialMatrixModel matrixModel = getMatrix(matrixName);
		if (matrixModel == null){
			throw new FelucaException("vector not found : '" + matrixName + "'! you need create it first");
		}
		//split fids[] into different-shard indexes(already do in retrieval);
		//split values[] then
		int[][] splittedIndexIds = matrixModel.splitFidsQuick();
		float[][][] splittedValues = matrixModel.splitValues(fids);
		
		//prepare parallel update
		ArrayList<Callable<Void>> mUpdateCallables = new ArrayList<Callable<Void>>( clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int[] toShardFids = splittedIndexIds[shardId];
			final float[][] toShardValues = splittedValues[shardId];
			final int toShardId = shardId;
			mUpdateCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
					matrixModels[toShardId].matrixUpdate(matrixName, toShardFids , toShardValues);	
					return null;
				}
			});

		}
		ConcurrentExecutor.execute(mUpdateCallables);
	}
	
	public synchronized  void dumpMatrix(final String matrixName, final String path) throws InterruptedException, ExecutionException{
		ArrayList<Callable<Void>> dumpCallables = new ArrayList<Callable<Void>>(clients.length);
		for(int shardId = 0; shardId < clients.length; shardId++){
			final int toShardId = shardId;
			dumpCallables.add(new Callable<Void>() {
				public Void call() throws Exception {
				     matrixModels[toShardId].matrixDump(matrixName, path, clients.length, toShardId);
				     return null ;
				}
			});
		}
		ConcurrentExecutor.execute(dumpCallables);
	}
	
}
