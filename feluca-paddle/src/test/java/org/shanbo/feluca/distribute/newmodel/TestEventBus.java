package org.shanbo.feluca.distribute.newmodel;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class TestEventBus {

	public static EventBus eventBus = new EventBus("test");  
	
	public static class EventListener {
	 
	    @Subscribe
	    public void listen(int[] event) throws InterruptedException {
	        Thread.sleep(100);
	        System.out.println(" " + event.length);
	    }
	 
	}
	
	static{
		eventBus.register(new EventListener());
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				for(int i = 0 ; i < 40; i++){
					eventBus.post(new int[i * 2]);
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
				}
			}
		});

		ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				for(int i = 0 ; i < 30; i++){
					eventBus.post(new int[i+ 1]);
				}
			}
		});
		System.out.println("!!!!!");
	}

}
