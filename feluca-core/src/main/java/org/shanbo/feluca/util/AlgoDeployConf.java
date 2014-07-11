package org.shanbo.feluca.util;

import org.shanbo.feluca.common.FelucaException;

import com.alibaba.fastjson.JSONObject;

public class AlgoDeployConf {
	private final boolean dataServer;
	private final boolean startingGun;
	private final boolean dataClient;
	private final boolean dataManager;
	
	public AlgoDeployConf(boolean isServer, boolean isStartingGun, boolean isClient, boolean isDataManager){
		this.dataServer = isServer;
		this.startingGun = isStartingGun;
		this.dataClient = isClient;
		this.dataManager = isDataManager;
		if (dataServer == false && startingGun == true ){
			throw new FelucaException("startingGun must be with dataServer");
		}
		if (dataClient == false && dataManager == true){
			throw new FelucaException("dataManager must be with dataClient");
		}
	}

	public boolean isDataServer() {
		return dataServer;
	}
	public boolean isStartingGun() {
		return startingGun;
	}
	public boolean isDataClient() {
		return dataClient;
	}
	public boolean isDataManager() {
		return dataManager;
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
				conf.getBooleanValue("dataServer"), 
				conf.getBooleanValue("startingGun"), 
				conf.getBooleanValue("dataClient"), 
				conf.getBooleanValue("dataManager"));
	}
	
	
	public static void main(String[] args) {
		String t = new AlgoDeployConf(true, false, true, false).toString();
		System.out.println(t);
		AlgoDeployConf parsed = AlgoDeployConf.parse(t);
		System.out.println(parsed);
	}
}
