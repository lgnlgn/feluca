package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.google.common.collect.ImmutableList;

public class TestReducer {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030", NetworkUtils.ipv4Host() + ":12040");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "rrr";
		
		GlobalConfig globalConfig = GlobalConfig.build("l2lr", DefaultAlgoConf.basicLRconf(20, 0.3, 0.1),
				dataName, DataUtils.loadForWorker(dataName),
				workers, models, 
				workers.get(0), thisDeployConf);

		
		GlobalConfig globalConfig2 = GlobalConfig.build("l2lr", DefaultAlgoConf.basicLRconf(20, 0.3, 0.1),
				dataName, DataUtils.loadForWorker(dataName),
				workers, models, 
				workers.get(1), new AlgoDeployConf(false, false, true, false));

		
		ReduceServer rs = new ReduceServer(new FloatReducerImpl(globalConfig), "l2lr", 12130);
		rs.start();
		
		final ReduceClient client = new ReduceClient(globalConfig);
		client.open();
		final ReduceClient client2 = new ReduceClient(globalConfig2);
		client2.open();

		ConcurrentExecutor.submit(new Runnable() {
			
			public void run() {
				float[] fetch;
				try {
					fetch = client.fetch(new float[]{1,2,3});
					System.out.println(TFloatArrayList.wrap(fetch).toString());
				} catch (InterruptedException e) {
				} catch (ExecutionException e) {
				}
			}
		});
		ConcurrentExecutor.submit(new Runnable() {
			
			public void run() {
				float[] fetch;
				try {
					fetch = client2.fetch(new float[]{5,6,7});
					System.out.println(TFloatArrayList.wrap(fetch).toString());
				} catch (InterruptedException e) {
				} catch (ExecutionException e) {
				}
			}
		});
		Thread.sleep(333333);
		client.close();
		client2.close();
		rs.stop();
	}

}
