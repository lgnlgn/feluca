package org.shanbo.feluca.distribute.launch;

import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.distribute.newmodel.VectorClient;
import org.shanbo.feluca.distribute.newmodel.VectorServer;
import org.shanbo.feluca.util.AlgoDeployConf;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;

public class LoopingBase {
	protected GlobalConfig conf;
	
	//data & computation
	LoopMonitor monitor;
	DataIterator dataIterator;
	protected VectorClient vectorClient;
	boolean isDataManager = false;
	
	//server & startingGun(with one of the server)
	private VectorServer vectorServer;
	StartingGun startingGun;

	
	
	public LoopingBase(GlobalConfig conf) throws Exception{
		init();
		
	}

	private void init() throws Exception{
		AlgoDeployConf deployConf = conf.getDeployConf();
		if (deployConf.isDataServer()){
			vectorServer = new VectorServer(conf, conf.getString("worker"));
			vectorServer.start();
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getModelServers().size());
		}
		if (deployConf.isDataClient()){
			vectorClient = new VectorClient(conf);
		}
		isDataManager = deployConf.isDataManager();
	}
	
	/**
	 * todo 
	 */
	private void close(){

		vectorClient.close();
		if (vectorServer != null)
			vectorServer.stop();
		if (startingGun != null)
			startingGun.close();
	}
	
}
