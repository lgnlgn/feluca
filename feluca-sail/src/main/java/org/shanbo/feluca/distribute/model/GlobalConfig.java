package org.shanbo.feluca.distribute.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.convert.DataStatistic;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class GlobalConfig {

	public final static  String  DATA_DETAIL = "dataStatus";
	public final static  String  MODEL_SERVERS = "modelServers";
	public final static  String  ALGO_CONF = "algorithm";
	public final static  String  PARTITIONER = "partitioner";
	
	final JSONObject conf ;
	final ImmutableList<String> modelServers;
	
	public GlobalConfig(String json){
		conf = JSONObject.parseObject(json);
		if (!conf.containsKey(ALGO_CONF) || !conf.containsKey(MODEL_SERVERS) || !conf.containsKey(DATA_DETAIL)){
			throw new FelucaException(String.format("config missing parameters : '%s' OR '%s' OR '%s'!", MODEL_SERVERS, ALGO_CONF, DATA_DETAIL));
		}
		ArrayList<String> servers = new ArrayList<String>();
		for(int i = 0 ; i  < conf.getJSONArray(MODEL_SERVERS).size(); i++){
			servers.add(conf.getJSONArray(MODEL_SERVERS).getString(i).split(":")[0]); //remove port, using default
		}
		modelServers = ImmutableList.copyOf(servers);
	}
	
	public int modelIndexOf(String address){
		return modelServers.indexOf(address);
	}
	
	public List<String> getModelServers(){
		return modelServers;
	}
	
	public Partitioner getPartitioner(){
		JSONObject partitioner = conf.getJSONObject(PARTITIONER);
		if (partitioner!= null && "range".equals(partitioner.getString("type")) ){
			return new Partitioner.RangePartitioner(conf.getJSONObject(DATA_DETAIL).getIntValue(DataStatistic.MAX_FEATURE_ID), modelServers.size());
		}else {
			return new Partitioner.HashPartitioner(modelServers.size());
		}
	}
	
	/**
	 * do not modify it
	 * @return
	 */
	public JSONObject getDataStatistic(){
		return conf.getJSONObject(DATA_DETAIL);
	}
	
	/**
	 * do not modify it
	 * @return
	 */
	public JSONObject getAlgorithmConf(){
		return conf.getJSONObject(ALGO_CONF);
	}
	
}
