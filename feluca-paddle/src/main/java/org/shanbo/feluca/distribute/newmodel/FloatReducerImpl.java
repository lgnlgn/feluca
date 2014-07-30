package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;

import java.util.concurrent.CountDownLatch;

import org.msgpack.rpc.loop.EventLoop;
import org.shanbo.feluca.distribute.launch.GlobalConfig;


public class FloatReducerImpl implements FloatReducer{
	class ReduceBag{
		int shardId;
		float[] values;
		
		public ReduceBag(int shardId,  float[] values){
			this.shardId = shardId;
			this.values = values;
		}
		
	}

	ReduceBag[] toReduce ;
	
	TFloatArrayList accValues;
	float[] results ;

	CountDownLatch latch ;
 	
	EventLoop loop;

	GlobalConfig globalConfig ;
	
	public FloatReducerImpl(GlobalConfig globalConfig){
		accValues = new TFloatArrayList();
		latch = new CountDownLatch(globalConfig.getWorkers().size());
		toReduce = new ReduceBag[globalConfig.getWorkers().size()];
	}

	
	
	public float[] reduce(int clientId, float[] values){
		toReduce[clientId] = new ReduceBag(clientId, values);
		latch.countDown(); //--
		try {
			latch.await();
			synchronized (FloatReducerImpl.class) {
				if (latch.getCount() == 0){
					accValues.resetQuick();
					for(int l = 0 ; l < toReduce[0].values.length ; l++){
						float accValue = toReduce[0].values[l];
						for(int i = 1 ; i < toReduce.length; i++){
							accValue = reduce(accValue, toReduce[i].values[l]);
						}
						accValues.add(accValue);
					}
					results = accValues.toArray();
					latch = new CountDownLatch(toReduce.length);
				}
			}
			return results;
		} catch (InterruptedException e) {
			return null;
		}

	}

	protected float reduce(float accValue, float newValue){
		return accValue + newValue;
	}


	public String getName() {
		return "sumReducer";
	}
}
