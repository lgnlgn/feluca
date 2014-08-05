package org.shanbo.feluca.distribute.classification.lr;

import java.util.List;
import java.util.Map;

import org.shanbo.feluca.distribute.launch.LoopingRunner;
import org.shanbo.feluca.paddle.DataUtils;
import org.shanbo.feluca.paddle.DefaultAlgoConf;
import org.shanbo.feluca.paddle.GlobalConfigGenerator;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestL2LR {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		String dataName = "real-sim";
		String worker = NetworkUtils.ipv4Host() + ":12030";
		JSONObject dataDistribution = new JSONObject();
		dataDistribution.put(worker,  ImmutableList.of("real-sim.v.0.dat"));
		
		JSONObject allocated = GlobalConfigGenerator.allocate(dataDistribution, "l2lr", 
				DefaultAlgoConf.basicLRconf(10, 0.4, 0.001), dataName, DataUtils.loadForWorker(dataName));
		
		LoopingRunner runner = new LoopingRunner(allocated.getJSONArray(worker));
		runner.runTasks(SGDL2LR.class);
	}

}
