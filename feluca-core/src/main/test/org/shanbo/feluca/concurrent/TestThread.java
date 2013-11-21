package org.shanbo.feluca.concurrent;

import java.util.concurrent.Future;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class TestThread {

	static class T {
		Thread keeper ;
		
		public T(){
			this.keeper = new Thread(new Runnable() {
				
				public void run() {
					while(true){
						System.out.print("!");
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
						}
					}
					
				}
			});
			this.keeper.setDaemon(true);
			this.keeper.start();
		}
		
	}
	
	
	
	public static void main(String[] args) throws InterruptedException {

		
		Future<?> submit = ConcurrentExecutor.submit(new Runnable() {
				
				public void run() {
					while(true){
						System.out.print("!");
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
						}
					}
					
				}
			});
		Thread.sleep(1000);
		submit.cancel(true);
		System.gc();
		Thread.sleep(4445);

	}

}
