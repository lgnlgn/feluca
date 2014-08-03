package org.shanbo.feluca.distribute.model.horizon;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

/**
 * <p>matrix still need testing;
 * <p>To use it: follow the 'fetch-then-update' way;
 * <p>needs delete method
 * @author lgn
 *
 */
public class ModelClient {

	public static class VectorBuffer{
		public float[] weights;
		TIntArrayList idBuffer;
		TFloatArrayList weightBuffer;
		int bufferSize;
		
		public VectorBuffer( int bufferSize, float[] weights){
			this.weights = weights;
			this.bufferSize = bufferSize;
			this.idBuffer = new TIntArrayList(512);
			this.weightBuffer = new TFloatArrayList(512);
		}
		
		public void clear(){
			if (idBuffer.size() > Math.max(512,bufferSize)){
				this.idBuffer = new TIntArrayList(512);
				this.weightBuffer = new TFloatArrayList(512);
			}else{
				this.idBuffer.resetQuick();
				this.weightBuffer.resetQuick();
			}
		}
		
		public void fidToBufer(int fid){
			this.idBuffer.add(fid);
			this.weightBuffer.add(weights[fid]);
		}
		
		public int[] getIds(){
			return idBuffer.toArray();
		}
		
		public float[] getWeights(){
			return weightBuffer.toArray();
		}
		
		public String toString(){
			return idBuffer.toString() + " : " + weightBuffer.toString();
		}
	}
	
	public static class MatrixBuffer{
		public float[][] matrix;
		TIntArrayList idBuffer;
		ArrayList<float[]> weightBuffer; //ref
		int bufferSize;
		
		public MatrixBuffer(int bufferSize, float[][] matrix){
			this.matrix = matrix;
			this.bufferSize = bufferSize;
			this.idBuffer = new TIntArrayList(512);
			this.weightBuffer = new ArrayList<float[]>(512);
		}
		
		public void clear(){
			if (idBuffer.size() > Math.max(512,bufferSize)){
				this.idBuffer = new TIntArrayList(512);
				this.weightBuffer = new ArrayList<float[]>(512);
			}else{
				this.idBuffer.resetQuick();
				this.weightBuffer.clear();
			}
		}
		
		public void fidToBufer(int fid){
			this.idBuffer.add(fid);
			this.weightBuffer.add(matrix[fid]);
		}
		
		public int[] getIds(){
			return idBuffer.toArray();
		}
		
		public float[][] getWeights(){
			float[][] result = new float[weightBuffer.size()][];
			for(int i = 0; i < weightBuffer.size(); i++){
				result[i] = weightBuffer.get(i);
			}
			return result;
		}
	}

	HashMap<String, VectorBuffer> vectorBuffers ; //vector size is determine by creator
	HashMap<String, MatrixBuffer> matrixBuffers;
	
	
	MModelImpl local;
	
	EventLoop loop;
	Client[] clients;
	MModel[] matrixModels;
	List<String> dataServerAddresses;
	
	int shardId;
	HashPartitioner partitioner;
	
	public ModelClient(List<String> dataServerAddresses, int shardId, MModelImpl local){
		
		this.dataServerAddresses = dataServerAddresses;
		this.shardId = shardId;
		this.local = local;
		loop = EventLoop.defaultEventLoop();
		clients = new Client[dataServerAddresses.size()];
		matrixModels = new MModel[dataServerAddresses.size()];
		partitioner = new HashPartitioner(dataServerAddresses.size());
		this.vectorBuffers = new HashMap<String, ModelClient.VectorBuffer>(3);
		this.matrixBuffers = new HashMap<String, ModelClient.MatrixBuffer>(3);
	}
	
	
	
	
	public void open() throws NumberFormatException, UnknownHostException{
		for(int i = 0; i < clients.length; i++){
			String[] hostPort = dataServerAddresses.get(i).split(":");
			clients[i] = new Client(hostPort[0], Integer.parseInt(hostPort[1]) + MModel.PORT_AWAY, loop);
			matrixModels[i] = clients[i].proxy(MModel.class);
		}
	}
	public synchronized void close(){
		for(Client client : clients){
			client.close();
		}
		loop.shutdown();
		System.out.println("modelClients of #" + clients.length + " all closed");
	}
	
	public void createVector(final String vectorName, int globalVectorSize, final float defaultValue, float vibration) throws InterruptedException, ExecutionException{
		local.vectorCreate(vectorName, globalVectorSize, defaultValue, vibration); //create to local
		vectorBuffers.put(vectorName, new VectorBuffer(512, local.vectors.get(vectorName))); //link to buffer
	}
	
	
	public float[] getVector(String vectorName){
		return local.vectors.get(vectorName);
	}
	
	public float[][] getMatrix(String matrixName){
		return local.matrixes.get(matrixName);
	}
	
	public List<Future<Integer>> vectorUpdate(final String vectorName, final  int[] fids) throws InterruptedException, ExecutionException{
		final VectorBuffer vector = vectorBuffers.get(vectorName);
		for(int i = 0 ; i < fids.length; i++){
			int toShardId = partitioner.decideShard(fids[i]);
			if (toShardId == shardId){
				vector.fidToBufer(fids[i]);
			}
		}
		// push vector to other servers;
		ArrayList<Callable<Integer>> pushCallables = new ArrayList<Callable<Integer>>();
		for(int i = 0 ; i < clients.length; i++){
			final int toShardId = i;
			if (i == shardId){ //local ; no need to push 
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						 return 1;
					}
				});
			}else{
				pushCallables.add(new Callable<Integer>() {
					public Integer call() throws Exception {
						return matrixModels[toShardId].vectorUpdate(vectorName, vector.getIds(), vector.getWeights());
					}
				});
			}
		}
		return ConcurrentExecutor.asyncExecute(pushCallables);
		
	}
	

	
	public void createMatrix( String matrixName, int globalRowSize, int columnSize, float defaultValue, float vibration) throws InterruptedException, ExecutionException{
		local.matrixCreate(matrixName, globalRowSize, columnSize, defaultValue, vibration);
		matrixBuffers.put(matrixName, new MatrixBuffer(512, local.matrixes.get(matrixName)));
	}

	
	
	public List<Future<Integer>> matrixUpdate(final String matrixName, final  int[] fids) throws InterruptedException, ExecutionException{
		final MatrixBuffer matrix = matrixBuffers.get(matrixName);
		for(int i = 0 ; i < fids.length; i++){
			int toShardId = partitioner.decideShard(fids[i]);
			if (toShardId == shardId){
				matrix.fidToBufer(fids[i]);
			}
		}
		ArrayList<Callable<Integer>> pushCallables = new ArrayList<Callable<Integer>>();
		for(int i = 0 ; i < clients.length; i++){
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
						return matrixModels[toShardId].matrixUpdate(matrixName, matrix.getIds(), matrix.getWeights());
					}
				});
			}
		}
		return ConcurrentExecutor.asyncExecute(pushCallables);
	}

	
}
