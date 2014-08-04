package org.shanbo.feluca.distribute.model;

import gnu.trove.list.array.TFloatArrayList;

import java.util.List;
import java.util.concurrent.Future;

import org.shanbo.feluca.distribute.model.horizon.MModelLocal;
import org.shanbo.feluca.distribute.model.horizon.MModelClient;
import org.shanbo.feluca.distribute.model.horizon.MModelServer;
import org.shanbo.feluca.util.NetworkUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.google.common.collect.ImmutableList;

public class TestSyncModel {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030", NetworkUtils.ipv4Host() + ":12730");
		
		MModelLocal model0 = new MModelLocal();
		MModelServer server0 = new MModelServer(workers.get(0), "h", model0);
		MModelClient mc0 = new MModelClient(workers, 0, model0);
		
		MModelLocal model1 = new MModelLocal();
		MModelServer server1 = new MModelServer(workers.get(1), "h", model1);
		MModelClient mc1 = new MModelClient(workers, 1, model1);
		

		//----------------------------------------
		server0.start();
		server1.start();
		
		mc0.createVector("a", 10, 0, 0);
		mc1.createVector("a", 10, 2, 0);
		mc0.connect();
		mc1.connect();
		
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
