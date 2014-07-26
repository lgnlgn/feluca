package org.shanbo.feluca.paddle;

import org.shanbo.feluca.common.FelucaException;

import com.alibaba.fastjson.JSONObject;

public class AlgoDeployConf {
	private final boolean modelServer;
	private final boolean startingGun;
	private final boolean modelClient;
	private final boolean modelManager;
	
	public AlgoDeployConf(boolean isModelServer, boolean isStartingGun, boolean isModelClient, boolean isModelManager){
		this.modelServer = isModelServer;
		this.startingGun = isStartingGun;
		this.modelClient = isModelClient;
		this.modelManager = isModelManager;
		if (modelClient == false && startingGun == true ){
			throw new FelucaException("startingGun must be with dataClient");
		}
		if (modelClient == false && modelManager == true){
			throw new FelucaException("dataManager must be with dataClient");
		}
	}


	

	public boolean isModelServer() {
		return modelServer;
	}



	public boolean isStartingGun() {
		return startingGun;
	}



	public boolean isModelClient() {
		return modelClient;
	}



	public boolean isModelManager() {
		return modelManager;
	}



	public String toString(){
		return JSONObject.toJSONString(this);
	}
	
	public static AlgoDeployConf parse(String json){
		JSONObject conf=  JSONObject.parseObject(json);
		return parse(conf);
	}

	public static AlgoDeployConf parse(JSONObject conf){
		return new AlgoDeployConf(
				conf.getBooleanValue("modelServer"), 
				conf.getBooleanValue("startingGun"), 
				conf.getBooleanValue("modelClient"), 
				conf.getBooleanValue("modelManager"));
	}
	
	
	public static void main(String[] args) {
		String t = new AlgoDeployConf(true, false, true, false).toString();
		System.out.println(t);
		AlgoDeployConf parsed = AlgoDeployConf.parse(t);
		System.out.println(parsed);
	}
}
