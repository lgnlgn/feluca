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
		models.add(NetworkUtils.getIPv4Localhost().toString());
		JSONObject algoConf = new JSONObject();
		GlobalConfig cc = GlobalConfig.build("sgdl2lr", new Partitioner.HashPartitioner(0), dataName,
				FileUtil.loadProperties(Constants.Base.getWorkerRepository() + "/" + dataName + "/" + dataName + ".sta"), algoConf, models );
	
	
	}

}
