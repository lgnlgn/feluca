package org.shanbo.feluca.distribute.cf.svd;

import java.util.List;

import org.shanbo.feluca.distribute.cf.stars.factorization.SVDModel;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestDistributeSVD {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "movielens_train";
		
		GlobalConfig globalConfig = GlobalConfig.build("cfsvd", DefaultAlgoConf.basicLRconf(10, 0.004, 0.01),
				dataName, DataUtils.loadForWorker(dataName),
				workers, models, 
				thisWorkerName, thisDeployConf);
		
		SVDModel model = new SVDModel(globalConfig);
		model.run();
		System.exit(0);
		
	}

}
