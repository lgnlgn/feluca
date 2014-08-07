package org.shanbo.feluca.distribute.cf;

import java.io.IOException;

import org.shanbo.feluca.distribute.cf.star.factorization.SVDModel;
import org.shanbo.feluca.distribute.launch.LoopingRunner;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.paddle.GlobalConfigGenerator;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

public class TestSVDModel {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String dataName = "mltrain";
		String worker = NetworkUtils.ipv4Host() + ":12030";
		JSONObject dataDistribution = new JSONObject();
		dataDistribution.put(worker,  ImmutableList.of("mltrain.v.0.dat","mltrain.v.1.dat"));
		
		JSONObject allocated = GlobalConfigGenerator.allocate(dataDistribution, "cfsvd", 
				DefaultAlgoConf.basicLRconf(10, 0.005, 0.002), dataName, DataUtils.loadForWorker(dataName));

		LoopingRunner runner = new LoopingRunner(allocated.getJSONArray(worker));
		runner.runTasks(SVDModel.class);
		System.exit(0);
	}

}
