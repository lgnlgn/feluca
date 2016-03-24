package org.shanbo.feluca.model;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;


public class FloatReducerImpl implements FloatReducer{

	/**
	 * 
	 * @author lgn
	 *
	 */
	static class Preparation{
		int shardId;
		float[][] values;

		public Preparation(int shardId,  float[][] values){
			this.shardId = shardId;
			this.values = values;
		}
	}
	
	static class Workshop{
		float[][][] reduceSlot; //ith, shardId, array
		
		float[][] result;
		
		public static abstract class Op{
			public abstract float[] action(float[][] data);
			
			public int getLength(float[][] data){
				int max = 0;
				for(int i = 0 ; i < data.length ; i++){
					if (data[i] != null){
						max = data[i].length;
					}
				}
				return max;
			}
		}
		
		public Workshop(Preparation[] input){
			if (input[0].values[0].length == 1 && input[0].values[0][1] == -1){//one degree
				reduceSlot = new float[1][][];
				reduceSlot[0] = new float[input.length][];
				result = new float[1][];
				for(int i = 0 ; i < input.length; i++){
					reduceSlot[0][i] = new float[input[0].values[1].length];
				}
			}else{ //two degree
				int maxSize = 0;
				for(int i = 0; i < input.length; i++){
					maxSize = Math.max((int)input[i].values[0][input[i].values[0].length -1 ], maxSize);
				}
				reduceSlot = new float[maxSize + 1][][];
				result = new float[maxSize + 1][];
				for(int i = 0 ; i < maxSize + 1; i++){ // fills 
					reduceSlot[i] = new float[input.length][];
				}
				for(int i = 0; i < input.length; i++){
					for(int j = 0 ; j < input[i].values[0].length; j++){
						int flatIndex = (int)input[i].values[0][j];
						reduceSlot[flatIndex][i] = new float[input[i].values[j + 1].length];
					}
				}
			}
		}
		
		
		void compute(Op op){
			for(int i = 0 ; i < reduceSlot.length; i++){
				result[i] = op.action(reduceSlot[i]); 
			}
		}
		
		void toReturn(Preparation[] output){
			if (output[0].values[0].length == 1 && output[0].values[0][1] == -1){//one degree
				for(int i = 0 ; i < output.length; i++){
					output[i].values[1] = result[1];
				}
			}else{//two degree
				for(int i = 0; i < output.length; i++){
					for(int j = 0 ; j < output[i].values[0].length; j++){
						int flatIndex = (int)output[i].values[0][j];
						output[i].values[j + 1] = result[flatIndex];
					}
				}
			}

		}
	}
	
	
	
	/**
	 * always single threaded-computing
	 * @author lgn
	 *
	 */
	static class ReduceProcessor{
		Preparation[] input ;

		Workshop workshop;
		
		CyclicBarrier enterBarrier ;
		CyclicBarrier leaveBarrier ;

		volatile boolean reduceDone;
		ReentrantLock lock = new ReentrantLock();

		public ReduceProcessor(int total){
			
			enterBarrier = new CyclicBarrier(total);
			leaveBarrier = new CyclicBarrier(total);
			input = new Preparation[total];
			
		}

		public void prepare(int clientId, float[][] values) throws InterruptedException{
			input[clientId] = new Preparation(clientId, values);
		}

		public void doReduce(Workshop.Op op){
			lock.lock();
			
			try{
				if (reduceDone == false){
					workshop = new Workshop(input);
					workshop.compute(op);
					workshop.toReturn(input);
					reduceDone = true;
					enterBarrier.reset();
					leaveBarrier.reset();
				}
			}finally{
				lock.unlock();
			}
		}


		public float[][] getResult(int shardId) throws InterruptedException, BrokenBarrierException{
			leaveBarrier.await();
			reduceDone = false; //it's ok; because fastest thread still have to wait for others to enter the doReduce()  
			return input[shardId].values;
		}

		public void waitForOther() throws InterruptedException, BrokenBarrierException{
			enterBarrier.await();
		}

	}
	
	HashMap<String, Workshop.Op> ops ;
	ReduceProcessor processor;
	
	public FloatReducerImpl(int total) {
		processor = new ReduceProcessor(total);
		ops = new HashMap<String, FloatReducerImpl.Workshop.Op>();
		ops.put("sum", new Workshop.Op() {
			public float[] action(float[][] data) {
				float[] result = new float[getLength(data)];
				for(int i = 0 ; i < result.length ; i++){
					for(int j = 0 ; j < data.length; j++){
						result[i] += (data[j] == null ? 0 : data[j][i]);
					}
				}
				return result;
			}
		});
	}
	
	@Override
	public float[][] reduce(String op, float[][] data, int shardId) {
		Workshop.Op operation = ops.get(op);
		if (operation == null)
			return null;
		try {
			processor.prepare(shardId, data);
			processor.waitForOther();
			processor.doReduce(operation);
			return processor.getResult(shardId);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
