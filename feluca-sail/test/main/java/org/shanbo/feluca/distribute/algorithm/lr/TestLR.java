package org.shanbo.feluca.distribute.algorithm.lr;

import java.util.List;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.sail.AlgoDeployConf;
import org.shanbo.feluca.sail.DefaultAlgoConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestLR {
	public static void main(String[] args) throws Exception {
		
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "real-sim";
		
		GlobalConfig globalConfig = GlobalConfig.build("l2lr", DefaultAlgoConf.basicAlgoConf(10),
				dataName, FileUtil.loadProperties("data/"+dataName+"/"+dataName+".sta"),
				workers, models, 
				thisWorkerName, thisDeployConf);
		
		SGDL2LR lr = new SGDL2LR(globalConfig);
		lr.run();

	}
}
