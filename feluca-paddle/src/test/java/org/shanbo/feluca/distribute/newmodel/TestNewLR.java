package org.shanbo.feluca.distribute.newmodel;

import java.io.IOException;
import java.util.List;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.lr2.SGDL2LR;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestNewLR {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "rrr";
		
		GlobalConfig globalConfig = GlobalConfig.build("l2lr", DefaultAlgoConf.basicLRconf(20, 0.3, 0.001),
				dataName, DataUtils.loadForWorker(dataName),
				workers, models, 
				thisWorkerName, thisDeployConf);
		
		SGDL2LR lr = new SGDL2LR(globalConfig);
		lr.run();
		System.exit(0);
	}

}
