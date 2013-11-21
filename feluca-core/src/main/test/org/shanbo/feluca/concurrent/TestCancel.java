package org.shanbo.feluca.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class TestCancel {
	static double c = 0;
	public static void run1(){
		
		for(int i = 0 ; i < 20000; i++){
			for(int j = 0 ; j < 300000;j++)
			   c += Math.sqrt(2.0);
		}
		System.out.println("finished");
	}
	
	
	public static void run2() throws InterruptedException{
		Thread.sleep(3000);
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		Runnable r1 = new Runnable() {
			
			public void run() {
				run1();
			}
		};
		
		Runnable r2 = new Runnable() {
			
			public void run() {
				try {
					run2();
				} catch (InterruptedException e) {
					System.out.println("interrupted!!!!!!!!!!!!!!");
				}
				
			}
		};
		
		
		Future<?> submit = ConcurrentExecutor.submit(r1);
		Thread.sleep(100);
		boolean cancel = submit.cancel(true);
		System.out.println(cancel);
//		submit.get();
		System.out.println("    done:" + submit.isDone() + "    iscancel:" + submit.isCancelled());
		System.out.println("sleep a while : " + c);
		Thread.sleep(2000);
		System.out.println("    done:" + submit.isDone() + "    iscancel:" + submit.isCancelled());
		System.out.println("  finally : " +c);
	}

}
