package org.shanbo.feluca.distribute.algorithm.lr;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.distribute.model.GlobalConfig;
import org.shanbo.feluca.distribute.model.Partitioner;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;

public class TestSGDL2LR {

	public static void main(String[] args) throws IOException {
		String dataName = "covtype";
		ArrayList<String> models = new ArrayList<String>(1);
		models.add(NetworkUtils.ipv4Host());
		JSONObject algoConf = new JSONObject();
		algoConf.put("alpha", 0.01);
		algoConf.put("lambda", 0.0001);
		
		GlobalConfig cc = GlobalConfig.build("sgdl2lr", new Partitioner.HashPartitioner(0), dataName,
				FileUtil.loadProperties(Constants.Base.getWorkerRepository() + Constants.Base.DATA_DIR +  "/" + dataName + "/" + dataName + ".sta"), algoConf, models );
	
		SGDL2LR lr = new SGDL2LR(cc);
		lr.init();
		lr.runAlgorithm();
		lr.close();
		System.out.println("finish all!");
	}

}
