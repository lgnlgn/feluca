package org.shanbo.feluca.distribute.algorithm.lr;

import java.util.List;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestLR {
	public static void main(String[] args) throws Exception {
		
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "real-sim";
		
		GlobalConfig globalConfig = GlobalConfig.build("l1lr", DefaultAlgoConf.basicLRconf(20, 0.3, 0.1),
				dataName, DataUtils.loadForWorker(dataName),
				workers, models, 
				thisWorkerName, thisDeployConf);
		
		SGDL1LR lr = new SGDL1LR(globalConfig);
		lr.run();
		System.exit(0);
	}
}
