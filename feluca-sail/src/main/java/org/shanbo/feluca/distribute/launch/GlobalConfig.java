package org.shanbo.feluca.distribute.launch;

import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.util.AlgoDeployConf;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

/**
 * this is just for computation;
 * @author lgn
 *
 */
public class GlobalConfig {
	public final static  String  ALGO_NAME  = "algoName";
	public final static  String  ALGO_CONF = "algoConf";
	
	public final static  String  DATA_STATUS = "dataStatus";
	
	public final static  String  DATA_SERVERS = "dataServers";
	public final static  String  WORKERS = "workers";

	public final static  String  MODEL_PREFIX = "modelPrifex";

	
	final JSONObject conf ;
	final ImmutableList<String> dataServers;
	final ImmutableList<String> workers;

	
	private GlobalConfig(String json){
		conf = JSONObject.parseObject(json);
		if (!conf.containsKey(ALGO_CONF) || !conf.containsKey(DATA_SERVERS) || !conf.containsKey(ALGO_NAME)){
			throw new FelucaException(String.format("config missing parameters : '%s' OR '%s' OR '%s'!", DATA_SERVERS, ALGO_CONF, ALGO_NAME));
		}
		if (!conf.containsKey(Constants.Algorithm.DATANAME) || !conf.containsKey(DATA_STATUS)){
			throw new FelucaException("config missing data parameters ");
		}
		dataServers = ImmutableList.copyOf(JSONUtil.JSONArrayToList(conf.getJSONArray(DATA_SERVERS)));
		workers = ImmutableList.copyOf(JSONUtil.JSONArrayToList(conf.getJSONArray(WORKERS)));
		
	}
	
	public static GlobalConfig parseJSON(String json){
		return new GlobalConfig(json);
	}
	
	public AlgoDeployConf getDeployConf(){
		return AlgoDeployConf.parse(conf.getJSONObject("deploy"));
	}
	
	public static GlobalConfig build(String algoName, JSONObject algoConf, 
			String dataName, Properties dataStatistic, 
			List<String> workers, List<String> dataServers,  
			String workerName, AlgoDeployConf deployConf){
		JSONObject json = new JSONObject();
		//-------algorithm
		json.put(ALGO_CONF, algoConf);
		json.put(ALGO_NAME, algoName);
		//---------data info
		json.put(Constants.Algorithm.DATANAME, dataName);
		JSONObject dataStatus = new JSONObject();
		for(Object key : dataStatistic.keySet()){
			dataStatus.put(key.toString(), dataStatistic.getProperty(key.toString()));
		}
		json.put(DATA_STATUS, dataStatus);
		//--------worker & dataServer
		json.put(DATA_SERVERS, dataServers);
		json.put("workers", workers);
		
		//------detail info
		json.put("deploy", deployConf);
		json.put("workerName", workerName);
		
		json.put(MODEL_PREFIX, dataName + "_model");
		return new GlobalConfig(json.toJSONString());
	}
	
	public String getAlgorithmName(){
		return conf.getString(ALGO_NAME);
	}
	
	
	public int modelIndexOf(String address){
		return dataServers.indexOf(address);
	}
	
	public List<String> getModelServers(){
		return dataServers;
	}
	public List<String> getWorkers(){
		return workers;
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
		if (ALGO_NAME.equals(key) || DATA_STATUS.equals(key)|| Constants.Algorithm.DATANAME.equals(key)||ALGO_CONF.equals(key) ){
			throw new FelucaException("your key '" + key+ "' is duplicated with require-keys");
		}else{
			conf.put(key, value);
		}
	}
	
	public String getString(String key){
		return conf.getString(key);
	}
	
	public String getWorkerName(){
		return conf.getString("workerName");
	}
	
	public String getDataName(){
		return getString(Constants.Algorithm.DATANAME);
	}
	
	
	public String toString(){
		return conf.toJSONString();
	}
	
	public String getModelPreifx(){
		return conf.getString(MODEL_PREFIX);
	}
}
