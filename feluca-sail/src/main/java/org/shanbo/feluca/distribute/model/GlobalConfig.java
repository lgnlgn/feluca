package org.shanbo.feluca.distribute.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.convert.DataStatistic;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

public class GlobalConfig {
	public final static  String  ALGO_NAME  = "jobName";
	public final static  String  DATA_STATUS = "dataStatus";
	public final static  String  MODEL_SERVERS = "modelServers";
	public final static  String  ALGO_CONF = "algoConf";
	public final static  String  PARTITIONER = "partitioner";
	
	final JSONObject conf ;
	final ImmutableList<String> modelServers;
	
	private GlobalConfig(String json){
		conf = JSONObject.parseObject(json);
		if (!conf.containsKey(ALGO_CONF) || !conf.containsKey(MODEL_SERVERS) || !conf.containsKey(ALGO_NAME)){
			throw new FelucaException(String.format("config missing parameters : '%s' OR '%s' OR '%s'!", MODEL_SERVERS, ALGO_CONF, ALGO_NAME));
		}
		if (!conf.containsKey(Constants.Algorithm.DATANAME) || !conf.containsKey(DATA_STATUS)){
			throw new FelucaException("config missing data parameters ");
		}
		ArrayList<String> servers = new ArrayList<String>();
		for(int i = 0 ; i  < conf.getJSONArray(MODEL_SERVERS).size(); i++){
			servers.add(conf.getJSONArray(MODEL_SERVERS).getString(i).split(":")[0]); //remove port, using default
		}
		modelServers = ImmutableList.copyOf(servers);
	}
	
	public static GlobalConfig parseJSON(String json){
		return new GlobalConfig(json);
	}
	
	public static GlobalConfig build(String algoName, Partitioner par, String dataName, Properties dataStatistic, JSONObject algoConf, List<String> modelServers){
		JSONObject json = new JSONObject();
		if (par instanceof Partitioner.RangePartitioner){
			json.put(PARTITIONER, "range");
		}else{
			json.put(PARTITIONER, "hash");
		}
		json.put(Constants.Algorithm.DATANAME, dataName);
		JSONArray ja = new JSONArray();
		ja.addAll(modelServers);
		json.put(MODEL_SERVERS, ja);
		JSONObject dataStatus = new JSONObject();
		for(Object key : dataStatistic.keySet()){
			dataStatus.put(key.toString(), dataStatistic.getProperty(key.toString()));
		}
		json.put(DATA_STATUS, dataStatus);
		json.put(ALGO_CONF, algoConf);
		json.put(ALGO_NAME, algoName);
		return new GlobalConfig(json.toJSONString());
	}
	
	public String getAlgorithmName(){
		return conf.getString(ALGO_NAME);
	}
	
	
	public int modelIndexOf(String address){
		return modelServers.indexOf(address);
	}
	
	public List<String> getModelServers(){
		return modelServers;
	}
	
	public Partitioner getPartitioner(){
		String partitioner = conf.getString(PARTITIONER);
		if (partitioner!= null && "range".equals(partitioner) ){
			return new Partitioner.RangePartitioner(conf.getJSONObject(DATA_STATUS).getIntValue(DataStatistic.MAX_FEATURE_ID), modelServers.size());
		}else {
			return new Partitioner.HashPartitioner(modelServers.size());
		}
	}
	
	/**
	 * do not modify it
	 * @return
	 */
	public JSONObject getDataStatistic(){
		return conf.getJSONObject(DATA_STATUS);
	}
	
	/**
	 * do not modify it
	 * @return
	 */
	public JSONObject getAlgorithmConf(){
		return conf.getJSONObject(ALGO_CONF);
	}
	
	public void putString(String key, String value){
		if (ALGO_NAME.equals(key) || DATA_STATUS.equals(key)|| Constants.Algorithm.DATANAME.equals(key)||ALGO_CONF.equals(key) || PARTITIONER.equals(key)){
			throw new FelucaException("your key '" + key+ "' is duplicated with require-keys");
		}else{
			conf.put(key, value);
		}
	}
	
	public String getString(String key){
		return conf.getString(key);
	}
	
	
	public String toString(){
		return conf.toJSONString();
	}
	
	
}
