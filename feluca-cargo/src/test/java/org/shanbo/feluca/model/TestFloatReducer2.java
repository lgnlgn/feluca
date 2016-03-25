package org.shanbo.feluca.model;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.shanbo.feluca.model.FloatObjects.OneDegree;
import org.shanbo.feluca.model.FloatObjects.TwoDegree;

public class TestFloatReducer2 {

	public static class LocalReduceClient2 extends FloatSumBarrier<TwoDegree>{

		FloatReducerImpl worker ;
		int shardId ;
		public LocalReduceClient2(int capacity, int shardId, int factor, FloatReducerImpl worker) {
			super(capacity, new TwoDegree(factor));
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
		
		final LocalReduceClient2 l1 = new LocalReduceClient2(5, 0, 3, reducerImpl);
		final LocalReduceClient2 l2 = new LocalReduceClient2(5, 1, 3, reducerImpl);
		final LocalReduceClient2 l3 = new LocalReduceClient2(5, 2, 3, reducerImpl);
	
		l1.get(0).setOne(1).setSigmaX(0, 2).setSigmaX(1, 11).setSigmaX2(2, 0.5f);
		l1.get(3).setOne(1).setSigmaX(0, 1).setSigmaX(1, 11).setSigmaX2(2, 0.5f).setSigmaX2(0, 1);
		
		l2.get(0).setOne(2).setSigmaX(0, 2).setSigmaX(1, 11).setSigmaX2(2, 0.5f);
		l2.get(1).setOne(1).setSigmaX(0, 2).setSigmaX(1, 11).setSigmaX2(2, 0.5f);
		l2.get(3).setOne(1).setSigmaX(0, 1).setSigmaX(1, 11).setSigmaX2(2, 0.5f).setSigmaX2(0, 1);

		l3.get(4).setOne(11).setSigmaX(0, 12).setSigmaX(1, 11).setSigmaX2(2, 0.5f);
		l3.get(3).setOne(2).setSigmaX(0, 14).setSigmaX(1, 11).setSigmaX2(2, 0.5f).setSigmaX2(1, 11);

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
		
		System.out.println(l1.window.zippedFormat());
		System.out.println(l2.window.zippedFormat());
		System.out.println(l3.window.zippedFormat());
		executor.shutdown();
	}
}
