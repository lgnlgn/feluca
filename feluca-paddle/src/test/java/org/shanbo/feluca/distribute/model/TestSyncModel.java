package org.shanbo.feluca.distribute.model;

import gnu.trove.list.array.TFloatArrayList;

import java.util.List;
import java.util.concurrent.Future;

import org.shanbo.feluca.distribute.model.horizon.MModelImpl;
import org.shanbo.feluca.distribute.model.horizon.ModelClient;
import org.shanbo.feluca.distribute.model.horizon.SyncModelServer;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.google.common.collect.ImmutableList;

public class TestSyncModel {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030", NetworkUtils.ipv4Host() + ":12730");
		
		MModelImpl model0 = new MModelImpl();
		SyncModelServer server0 = new SyncModelServer(workers.get(0), "h", model0);
		ModelClient mc0 = new ModelClient(workers, 0, model0);
		
		MModelImpl model1 = new MModelImpl();
		SyncModelServer server1 = new SyncModelServer(workers.get(1), "h", model1);
		ModelClient mc1 = new ModelClient(workers, 1, model1);
		

		//----------------------------------------
		server0.start();
		server1.start();
		
		mc0.createVector("a", 10, 0, 0);
		mc1.createVector("a", 10, 2, 0);
		mc0.open();
		mc1.open();
		
		List<Future<Integer>> vectorUpdate0 = mc0.vectorUpdate("a", new int[]{0,2,4});
		List<Future<Integer>> vectorUpdate1 = mc1.vectorUpdate("a", new int[]{1,3,5});
		
		ConcurrentExecutor.get(vectorUpdate0);
		ConcurrentExecutor.get(vectorUpdate1);
		
		System.out.println(TFloatArrayList.wrap(mc0.getVector("a")).toString());
		System.out.println(TFloatArrayList.wrap(mc1.getVector("a")).toString());
		mc0.close();
		mc1.close();
		//-------------
		server0.stop();
		server1.stop();
	}

}
