package org.shanbo.feluca.model;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.shanbo.feluca.model.FloatObjects.OneDegree;

public class TestFloatReducer {

	public static class LocalReduceClient extends FloatSumBarrier<OneDegree>{

		FloatReducerImpl worker ;
		int shardId ;
		public LocalReduceClient(int capacity, int shardId, FloatReducerImpl worker) {
			super(capacity, new OneDegree());
			this.worker = worker;
			this.shardId = shardId;
		}

		@Override
		public void call() {
			float[][] reduce = worker.reduce("sum", window.zip(), shardId);
			window.unzipAndSet(reduce);
		}
		
		public String toString(){
			return window.toString();
		}
	}


	
	
	public static void main(String[] args) {
		FloatReducerImpl reducerImpl = new FloatReducerImpl(3);
		
		final LocalReduceClient l1 = new LocalReduceClient(5, 0, reducerImpl);
		final LocalReduceClient l2 = new LocalReduceClient(5, 1, reducerImpl);
		final LocalReduceClient l3 = new LocalReduceClient(5, 2, reducerImpl);
	
		l1.get(0).set(10);
		l1.get(3).set(10);
	
		l2.get(1).set(10);
		l2.get(3).set(10);
		
		l3.get(4).set(110);
		l3.get(3).set(10);
		System.out.println(l1.toString());
		System.out.println(l2.toString());
		System.out.println(l3.toString());
		
		System.out.println("========");
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.submit(new Runnable() {
			public void run() {
				l1.call();
			}
		});
		executor.submit(new Runnable() {
			public void run() {
				l2.call();
			}
		});
		l3.call();
		
		System.out.println(l1.toString());
		System.out.println(l2.toString());
		System.out.println(l3.toString());
		executor.shutdown();
	}
}
