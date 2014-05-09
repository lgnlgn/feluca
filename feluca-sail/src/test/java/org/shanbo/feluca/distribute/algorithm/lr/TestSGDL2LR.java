package org.shanbo.feluca.distribute.algorithm.lr;

import java.net.UnknownHostException;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;

public class TestSGDL2LR {

	public static void main(String[] args) throws UnknownHostException {
		JSONObject conf = new JSONObject();
		JSONObject algoConf = (JSONObject) conf.put(NetworkUtils.getIPv4Localhost().toString(), new JSONObject());
		algoConf.put(Constants.Algorithm.DATANAME, "covtype");
	}

}
