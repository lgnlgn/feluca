package org.shanbo.feluca.distribute.model;

import java.util.HashMap;
import java.util.Set;

import org.shanbo.feluca.common.FelucaException;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableSet;

public class GlobalConfig {

	HashMap<String, JSONObject> ipMap;
	JSONObject[] allConfigs;
	
	public int nodes(){
		return allConfigs.length;
	}
	
	public GlobalConfig(String json){
		JSONObject tmp = JSONObject.parseObject(json);
		int i = 0;
		ipMap = new HashMap<String, JSONObject>();
		allConfigs = new JSONObject[tmp.size()];
		for(String keyString : tmp.keySet()){
			ipMap.put(keyString, tmp.getJSONObject(keyString));
			allConfigs[i] = tmp.getJSONObject(keyString);
			allConfigs[i].put("address", keyString);
			i+=1;
		}
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
	
	public Set<String> addresses(){
		return ImmutableSet.copyOf(ipMap.keySet());
	}
	
}
