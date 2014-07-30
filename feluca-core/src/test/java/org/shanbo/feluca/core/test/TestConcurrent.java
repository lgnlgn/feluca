package org.shanbo.feluca.core.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class TestConcurrent {

	/**
	 * @param args
	 * @throws TimeoutException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException  {
		Future<?> submit = ConcurrentExecutor.submit(new Runnable() {
			
			@Override
			public void run() {
				int l = 0;;
				while(true){
					try {
						Thread.sleep(10);
						l ++ ;
					} catch (InterruptedException e) {
						System.out.println("interrupted? " + l);
						break;
					}
				}
				System.out.println("finish");
			}
		});
		Thread.sleep(1111);
		try {
			submit.get(100, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			//nothing
		} catch (TimeoutException e) {
			submit.cancel(true);
		}
	}

}
