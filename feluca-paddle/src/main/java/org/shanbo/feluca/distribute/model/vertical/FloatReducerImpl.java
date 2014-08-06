package org.shanbo.feluca.distribute.model.vertical;

import gnu.trove.list.array.TFloatArrayList;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FloatReducerImpl implements FloatReducer{

	static Logger log = LoggerFactory.getLogger(FloatReducerImpl.class);

	static class ReducePreparation{
		int shardId;
		float[] values;

		public ReducePreparation(int shardId,  float[] values){
			this.shardId = shardId;
			this.values = values;
		}

	}


	abstract static class ReduceProcessor{
		ReducePreparation[] toReduce ;	
		TFloatArrayList accValues;
		volatile float[] results ;
		
		CyclicBarrier enterBarrier ;
		CyclicBarrier leaveBarrier ;
		
		volatile boolean reduceDone;
		ReentrantLock lock = new ReentrantLock();

		public ReduceProcessor(int total){
			accValues = new TFloatArrayList();
			enterBarrier = new CyclicBarrier(total);
			leaveBarrier = new CyclicBarrier(total);
			toReduce = new ReducePreparation[total];
		}

		public void prepare(int clientId, float[] values) throws InterruptedException{
			toReduce[clientId] = new ReducePreparation(clientId, values);
		}

		public void doReduce(){
			lock.lock();
			if (reduceDone == false){
				accValues.resetQuick();
				processValues();
				results = accValues.toArray();
				reduceDone = true;
				enterBarrier.reset();
				leaveBarrier.reset();
			}
			lock.unlock();
		}

		public abstract void processValues() ;

		public float[] getResult() throws InterruptedException, BrokenBarrierException{
			leaveBarrier.await();
			reduceDone = false;
			return results;
		}

		public void waitForOther() throws InterruptedException, BrokenBarrierException{
			enterBarrier.await();
		}

	}

	HashMap<String, ReduceProcessor> reducers;

	public FloatReducerImpl(int totalClients){
		reducers = new HashMap<String, FloatReducerImpl.ReduceProcessor>(6);
		reducers.put("sum", new SumReducer(totalClients));
		reducers.put("avg", new AvgReducer(totalClients));
		reducers.put("max", new MaxReducer(totalClients));
		reducers.put("min", new MinReducer(totalClients));

	}


	public float[] reduce(String name, int clientId, float[] values){
		ReduceProcessor processor = reducers.get(name);
		if (processor == null)
			return null;
		try {
			processor.prepare(clientId, values);
			processor.waitForOther();
			processor.doReduce();
			return processor.getResult();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}


	public String getName() {
		return "floatReducer";
	}


	public static class SumReducer extends ReduceProcessor{

		public SumReducer(int total) {
			super(total);
		}

		@Override
		public void processValues() {
			for(int l = 0 ; l < toReduce[0].values.length ; l++){ //each input 
				float accValue = toReduce[0].values[l];   
				for(int i = 1 ; i < toReduce.length; i++){    //merge from other
					accValue += toReduce[i].values[l];
				}
				accValues.add(accValue);
			}
		}

	}

	public static class AvgReducer extends ReduceProcessor{

		public AvgReducer(int total) {
			super(total);
		}

		@Override
		public void processValues() {
			for(int l = 0 ; l < toReduce[0].values.length ; l++){ //each input 
				float accValue = toReduce[0].values[l];   
				for(int i = 1 ; i < toReduce.length; i++){    //merge from other
					accValue += toReduce[i].values[l];
				}
				accValues.add( accValue / toReduce.length);
			}
		}

	}

	public static class MinReducer extends ReduceProcessor{

		public MinReducer(int total) {
			super(total);
		}

		@Override
		public void processValues() {
			for(int l = 0 ; l < toReduce[0].values.length ; l++){ //each input 
				float accValue = toReduce[0].values[l];   
				for(int i = 1 ; i < toReduce.length; i++){    //merge from other
					accValue = Math.min(accValue, toReduce[i].values[l]);
				}
				accValues.add(accValue);
			}
		}

	}

	public static class MaxReducer extends ReduceProcessor{

		public MaxReducer(int total) {
			super(total);
		}

		@Override
		public void processValues() {
			for(int l = 0 ; l < toReduce[0].values.length ; l++){ //each input 
				float accValue = toReduce[0].values[l];   
				for(int i = 1 ; i < toReduce.length; i++){    //merge from other
					accValue = Math.max(accValue, toReduce[i].values[l]);
				}
				accValues.add(accValue);
			}
		}

	}
}
