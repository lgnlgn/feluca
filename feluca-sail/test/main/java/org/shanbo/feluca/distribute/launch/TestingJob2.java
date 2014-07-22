package org.shanbo.feluca.distribute.launch;

import java.util.List;

import org.shanbo.feluca.sail.AlgoDeployConf;
import org.shanbo.feluca.sail.DefaultAlgoConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestingJob2 {
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030", NetworkUtils.ipv4Host() + ":12031");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130", NetworkUtils.ipv4Host() + ":12131");
		String thisWorkerName = workers.get(1);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, false, true, false);
		
		GlobalConfig globalConfig = GlobalConfig.build("sleep", DefaultAlgoConf.basicAlgoConf(4),
				"covtype", FileUtil.loadProperties("data/covtype/covtype.sta"),
				workers, models, 
				thisWorkerName, thisDeployConf);
		TestingJob.SleepJob sj = new TestingJob.SleepJob(globalConfig);
		sj.run();
	}

}
