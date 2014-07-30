package org.shanbo.feluca.distribute.newmodel;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;

public class TestMonitor {
	
	static boolean ok = false;
	
	public static void main(String[] args) throws InterruptedException {
		
		
		
		Monitor monitor = new Monitor();
		Guard g = new Guard(monitor) {
			
			@Override
			public boolean isSatisfied() {
				return (ok == true);
			}
		};
		
		System.out.println("test");
		new Thread(new Runnable() {
			
			public void run() {
				try {
					Thread.sleep(3333);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ok = true;
			}
		}).start();
		new Thread(new Runnable() {
			public void run() {
				while(true){
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("ok:" + ok);
				}
			}
		}).start();
		System.out.println("1111");
	}
}
