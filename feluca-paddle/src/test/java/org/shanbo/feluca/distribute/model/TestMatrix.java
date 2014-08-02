package org.shanbo.feluca.distribute.model;

import java.io.IOException;
import java.util.List;

import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.model.old.ModelClient;
import org.shanbo.feluca.distribute.model.old.ModelServer;
import org.shanbo.feluca.distribute.model.old.PartialMatrixModel;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.google.common.collect.ImmutableList;

public class TestMatrix {

	public static void incrAll(float[] values, float v){
		for(int i = 0 ; i < values.length; i++){
			values[i] += 3.0f;
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		List<String> workers = ImmutableList.of(NetworkUtils.ipv4Host() + ":12030");
		List<String> models = ImmutableList.of(NetworkUtils.ipv4Host() + ":12130");
		String thisWorkerName = workers.get(0);
		AlgoDeployConf thisDeployConf = new AlgoDeployConf(true, true, true, true);
		
		String dataName = "real-sim";
		
		GlobalConfig globalConfig = GlobalConfig.build("matrix", DefaultAlgoConf.basicLRconf(20, 0.3, 0.1),
				dataName, FileUtil.loadProperties("data/"+dataName+"/"+dataName+".sta"),
				workers, models, 
				thisWorkerName, thisDeployConf);
		
		
		ModelServer ms = new ModelServer(globalConfig);
		ms.start();
		
		ModelClient client = new ModelClient(globalConfig);
		
		client.open();
		System.out.println("----------------------");
		String matrixName = "testMatrix";
		int[] fids = new int[]{1,3,5,6,8};
		client.createMatrix(matrixName, 50, 20, 0, 0.5f);
		PartialMatrixModel matrixRetrieve = client.matrixRetrieve(matrixName, fids);
		float[] fs = matrixRetrieve.get(1);
		incrAll(fs, 1f);
		float[] fs2 = matrixRetrieve.get(3);
		incrAll(fs2, 3f);
		client.matrixUpdate(matrixName, fids);
		
		matrixRetrieve = client.matrixRetrieve(matrixName, fids);
		fs = matrixRetrieve.get(1);
		incrAll(fs, 5f);
		client.matrixUpdate(matrixName, fids);
		
		client.dumpMatrix(matrixName, "data/" + matrixName + ".dat");
		client.close();
		
		
		ms.stop();
		
	}

}
