package org.shanbo.feluca.paddle;

import com.alibaba.fastjson.JSONObject;

public class AlgoDeployConf {
	private final boolean reduceServer;
	private final boolean startingGun;
	
	public AlgoDeployConf(boolean reduceServer, boolean isStartingGun){
		this.reduceServer = reduceServer;
		this.startingGun = isStartingGun;
	}


	

	public boolean isReduceServer() {
		return reduceServer;
	}



	public boolean isStartingGun() {
		return startingGun;
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
				conf.getBooleanValue("reduceServer"), 
				conf.getBooleanValue("startingGun"));
	}
	
	
	public static void main(String[] args) {
		String t = new AlgoDeployConf(true, false).toString();
		System.out.println(t);
		AlgoDeployConf parsed = AlgoDeployConf.parse(t);
		System.out.println(parsed);
	}
}
