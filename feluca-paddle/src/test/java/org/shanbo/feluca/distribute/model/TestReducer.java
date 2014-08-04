package org.shanbo.feluca.distribute.model;

import gnu.trove.list.array.TFloatArrayList;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.distribute.model.vertical.FloatReducerClient;
import org.shanbo.feluca.distribute.model.vertical.ReduceServer;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.google.common.collect.ImmutableList;

public class TestReducer {

	/**
	 * @param args
	 * @throws SocketException 
	 * @throws UnknownHostException 
	 * @throws NumberFormatException 
	 * @throws InterruptedException 
	 * @throws ExecutionException 
	 */
	public static void main(String[] args) throws SocketException, NumberFormatException, UnknownHostException, InterruptedException, ExecutionException {
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030",NetworkUtils.ipv4Host() + ":12730");

		ReduceServer rServer0 = new ReduceServer(workers.get(0), workers.size(), "ha");
		ReduceServer rServer1 = new ReduceServer(workers.get(1), workers.size(), "ha");
		
		rServer0.start();
		rServer1.start();
		
		//-----------
		final FloatReducerClient client0 = new FloatReducerClient(workers, 0);
		client0.connect();
		final FloatReducerClient client1 = new FloatReducerClient(workers, 1);
		client1.connect();
		
		//--------
		System.out.println("-------------------------");
		ConcurrentExecutor.submit(new Runnable() {
			
			public void run() {
				float[] fetch;
				try {
					fetch = client0.avg(new float[]{1,2,3,7,8});
					System.out.println(TFloatArrayList.wrap(fetch).toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		ConcurrentExecutor.submit(new Runnable() {
			
			public void run() {
				float[] fetch;
				try {
					fetch = client1.avg(new float[]{5,6,7,0,0});
					System.out.println(TFloatArrayList.wrap(fetch).toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("=========================");
		Thread.sleep(333333);
		client0.close();
		client1.close();
		
		rServer0.stop();
		rServer1.stop();
	}

}
