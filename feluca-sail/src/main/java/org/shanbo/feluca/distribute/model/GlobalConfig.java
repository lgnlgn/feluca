package org.shanbo.feluca.distribute.model;

import java.util.HashMap;

import org.shanbo.feluca.common.FelucaException;

import com.alibaba.fastjson.JSONObject;

public class GlobalConfig {

	HashMap<String, JSONObject> ipMap;
	JSONObject[] allConfigs;
	
	public int nodes(){
		return allConfigs.length;
	}
	
	/**
	 * 0 to size-1
	 * @param index
	 * @return
	 */
	public JSONObject getConfigByPart(int index){
		if (index <0)
			throw new FelucaException("index must > 0", new IllegalArgumentException());
		if (index >= allConfigs.length)
			throw new FelucaException("getConfigByPart index out of range", new IndexOutOfBoundsException());
		return JSONObject.parseObject(allConfigs[index].toJSONString());
	}
	
	public JSONObject getConfigByNodeAddress(String hostAndPort){
		JSONObject jo = ipMap.get(hostAndPort);
		if (jo == null){
			throw new FelucaException("Host not found");
		}
		return JSONObject.parseObject(jo.toJSONString());
	}
}
