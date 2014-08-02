package org.shanbo.feluca.distribute.model.horizon;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.model.horizon.ModelCore.MatrixModel;
import org.shanbo.feluca.distribute.model.horizon.ModelCore.VectorModel;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncModelLocal implements SyncModel{

	static Logger log = LoggerFactory.getLogger(SyncModelLocal.class);
	ModelCore model;

	EventLoop loop;
	Client[] clients;
	SyncModel[] syncModels;
	List<String> dataServerAddresses;

	int shardId;
	HashPartitioner partitioner;

	public SyncModelLocal(GlobalConfig globalConfig){
		loop = EventLoop.defaultEventLoop();
		clients = new Client[globalConfig.getModelServers().size()];
		syncModels = new SyncModel[globalConfig.getModelServers().size()];
		dataServerAddresses =  globalConfig.getModelServers();	
		partitioner = new HashPartitioner(dataServerAddresses.size());
	}

	public List<Future<Integer>> pushVector(final String vectorName, int[] fids) {
		final VectorModel vector = model.getVector(vectorName);
		for(int i = 0 ; i < fids.length; i++){
			int toShardId = partitioner.decideShard(fids[i]);
			if (toShardId == shardId){
				vector.fidToBufer(fids[i]);
			}
		}
		ArrayList<Callable<Integer>> pushCallables = new ArrayList<Callable<Integer>>();
		for(int i = 0 ; i < syncModels.length; i++){
			final int toShardId = i;
			if (i == shardId){ //local ; no push 
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						 return 1;
					}
				});
			}else{
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						return syncModels[toShardId].vectorUpdate(vectorName, vector.getIds(), vector.getWeights());
					}
				});
			}
		}
		return ConcurrentExecutor.asyncExecute(pushCallables);
	}


	public List<Future<Integer>> pushMatrix(final String matrixName, int[] fids) {
		final MatrixModel matrix = model.getMatrix(matrixName);
		for(int i = 0 ; i < fids.length; i++){
			int toShardId = partitioner.decideShard(fids[i]);
			if (toShardId == shardId){
				matrix.fidToBufer(fids[i]);
			}
		}
		ArrayList<Callable<Integer>> pushCallables = new ArrayList<Callable<Integer>>();
		for(int i = 0 ; i < syncModels.length; i++){
			final int toShardId = i;
			if (i == shardId){ //local ; no push 
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						 return 1;
					}
				});
			}else{
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						return syncModels[toShardId].matrixUpdate(matrixName, matrix.getIds(), matrix.getWeights());
					}
				});
			}
		}
		return ConcurrentExecutor.asyncExecute(pushCallables);

	}

	public float[] getVector(String vectorName){
		return model.getVector(vectorName).weights;
	}

	public float[][] getMatrix(String matrixName){
		return model.getMatrix( matrixName).matrix;
	}

	public void open() throws NumberFormatException, UnknownHostException{
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = dataServerAddresses.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]), loop);
			syncModels[i] = clients[i].proxy(SyncModel.class);
		}
	}
	public void close(){
		for(Client client : clients){
			client.close();
		}
		loop.shutdown();
		System.out.println("modelClients of #" + clients.length + " all closed");
	}



	public int vectorUpdate(String vectorName, int[] fids, float[] values) {
		float[] vector = getVector(vectorName);
		if (vector != null){
			for(int i = 0; i < fids.length; i ++){
				vector[fids[i]] = values[i];
			}
			return 1;
		}
		return 0;
	}

	public int matrixUpdate(String matrixName, int[] fids, float[][] values) {
		float[][] matrix = getMatrix(matrixName);
		if (matrix != null){
			for(int i = 0; i < fids.length; i ++){
				float[] column = matrix[fids[i]];
				float[] newValue = values[i];
				for(int j = 0 ; j < column.length; j++)
					column[j] = newValue[j];
			}
			return 1;
		}
		return 0;
	}
	
	public void createVector(String vectorName, int weightsLength, int bufferSize, float defaultValue, float vibration){
		model.createVector(vectorName, weightsLength, bufferSize, defaultValue, vibration);
	}
	
	public void createMatrix(String matrixName, int rowSize, int columnSize, int bufferSize, float defaultValue, float vibration){
		model.createMatrix(matrixName, rowSize, columnSize, bufferSize, defaultValue, vibration);
	}
}
