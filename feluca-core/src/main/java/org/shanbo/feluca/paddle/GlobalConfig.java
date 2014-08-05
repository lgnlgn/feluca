package org.shanbo.feluca.paddle;

import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

/**
 * this is just for computation;
 * @author lgn
 *
 */
public class GlobalConfig {
	public final static  String  ALGO_NAME  = "algorithmName";
	public final static  String  ALGO_CONF = "algorithmConf";
	
	public final static  String  DATA_STAT = "dataStatistic";
	public final static  String  SHARD_ID = "shardId";
	public final static  String  REDUCE_SERVERS = "reduceServers";
	public final static  String  WORKERS = "workers";


	
	final JSONObject conf ;
	final ImmutableList<String> reduceServers;
	final ImmutableList<String> workers;

	
	private GlobalConfig(String json){
		conf = JSONObject.parseObject(json);
		if (!conf.containsKey(ALGO_CONF) || !conf.containsKey(REDUCE_SERVERS) || !conf.containsKey(ALGO_NAME)){
			throw new FelucaException(String.format("config missing parameters : '%s' OR '%s' OR '%s'!", REDUCE_SERVERS, ALGO_CONF, ALGO_NAME));
		}
		if (!conf.containsKey(Constants.Algorithm.DATANAME) || !conf.containsKey(DATA_STAT)){
			throw new FelucaException("config missing data parameters ");
		}
		reduceServers = ImmutableList.copyOf(JSONUtil.JSONArrayToList(conf.getJSONArray(REDUCE_SERVERS)));
		workers = ImmutableList.copyOf(JSONUtil.JSONArrayToList(conf.getJSONArray(WORKERS)));
		
	}
	
	public static GlobalConfig parseJSON(String json){
		return new GlobalConfig(json);
	}
	
	public AlgoDeployConf getDeployConf(){
		return AlgoDeployConf.parse(conf.getJSONObject("deployConf"));
	}
	
	public static GlobalConfig build(int shardId, String algoName, JSONObject algoConf, 
			String dataName, Properties dataStatistic, 
			List<String> workers, List<String> modelServers,  
			String workerName, AlgoDeployConf deployConf){
		JSONObject json = new JSONObject();
		//-------algorithm
		json.put(SHARD_ID, shardId);
		json.put(ALGO_CONF, algoConf);
		json.put(ALGO_NAME, algoName);
		//---------data info
		json.put(Constants.Algorithm.DATANAME, dataName);
		JSONObject dataStatus = new JSONObject();
		for(Object key : dataStatistic.keySet()){
			dataStatus.put(key.toString(), dataStatistic.getProperty(key.toString()));
		}
		json.put(DATA_STAT, dataStatus);
		//--------worker & dataServer
		json.put(REDUCE_SERVERS, modelServers);
		json.put("workers", workers);
		
		//------detail info
		json.put("deployConf", deployConf);
		json.put("workerName", workerName);

		return new GlobalConfig(json.toJSONString());
	}
	
	public String getAlgorithmName(){
		return conf.getString(ALGO_NAME);
	}
	
	
	public int modelIndexOf(String address){
		return reduceServers.indexOf(address);
	}
	
	public List<String> getReduceServers(){
		return reduceServers;
	}
	public List<String> getWorkers(){
		return workers;
	}
	
	/**
	 * Becareful: values are String type, you have to parse by yourself;
	 * @return
	 */
	public JSONObject getDataStatistic(){
		return JSONObject.parseObject(conf.getJSONObject(DATA_STAT).toJSONString());
	}
	
	/**
	 * 
	 * @return
	 */
	public JSONObject getAlgorithmConf(){
		return JSONObject.parseObject(conf.getJSONObject(ALGO_CONF).toJSONString());
	}
	
	
	public void putString(String key, String value){
		if (ALGO_NAME.equals(key) || DATA_STAT.equals(key)|| Constants.Algorithm.DATANAME.equals(key)||ALGO_CONF.equals(key) ){
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
	
	public int getShardId(){
		return conf.getIntValue(SHARD_ID);
	}
}
