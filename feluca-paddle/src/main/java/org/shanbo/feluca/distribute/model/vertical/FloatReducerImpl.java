package org.shanbo.feluca.distribute.model.vertical;

import gnu.trove.list.array.TFloatArrayList;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
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
		float[] results ;
		CountDownLatch latch ;
		
		Object lock = new Object();
		
		public ReduceProcessor(int total){
			accValues = new TFloatArrayList();
			latch = new CountDownLatch(total);
			toReduce = new ReducePreparation[total];
		}
		
		public void prepare(int clientId, float[] values){
			toReduce[clientId] = new ReducePreparation(clientId, values);
		}
		
		public void doReduce(){
			synchronized (lock) {
				if (latch.getCount() == 0){
					accValues.resetQuick();
					processValues();
					results = accValues.toArray();
					latch = new CountDownLatch(toReduce.length);
				}
			}
		}
		
		public abstract void processValues() ;
		
		public float[] getResult(){
			return results;
		}
		
		public void countDownAwait() throws InterruptedException{
			latch.countDown(); //--
			latch.await();
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
			processor.countDownAwait();
			processor.doReduce();
			return processor.getResult();
		} catch (InterruptedException e) {
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
