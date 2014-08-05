package org.shanbo.feluca.paddle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.util.FileUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class GlobalConfigGenerator {
	
	private static Integer extractShardId(String dataBlockName){
		String part = dataBlockName.split(".v.")[1];
		return Integer.parseInt(part.substring(0, part.indexOf(".dat")));
	}
	
	public static JSONObject allocate(JSONObject dataDistribution
			,String algoName, JSONObject algoConf, String dataName, Properties dataStatistic){
		List<String> reducers = Lists.newArrayList(dataDistribution.keySet());
		TreeMap<Integer, String> shard2Worker = new TreeMap<Integer, String>();
		int maxShard = 0;
		for(String node : dataDistribution.keySet()){
			int port = Integer.parseInt(node.split(":")[1]);
			JSONArray blocks = dataDistribution.getJSONArray(node);
			for(int i = 0 ; i < blocks.size(); i++){
				int shardId = extractShardId(blocks.getString(i));
				maxShard = Math.max(maxShard, shardId);
				shard2Worker.put(shardId, node.split(":")[0] + ":" + (port + i * 100));
			}
		}
		
		ArrayList<String> workerAddresses = Lists.newArrayList();
		ArrayList<AlgoDeployConf> configs = Lists.newArrayList();
		HashSet<String> tmpNodes = Sets.newHashSet(dataDistribution.keySet());
		boolean startingGun = true;
		for(Entry<Integer, String> entry : shard2Worker.entrySet()){
			boolean isReduce = tmpNodes.remove(entry.getValue());
			AlgoDeployConf deployConf = new AlgoDeployConf(isReduce, (startingGun && true));
			startingGun = false;
			//
			workerAddresses.add(entry.getValue());
			configs.add(deployConf);
		}
		if (workerAddresses.size() - 1 != maxShard){
			throw new FelucaException("data integrity problem! Blocks missing maxShardID:[" + maxShard + "] but we found only " + (workerAddresses.size()-1) );
		}
		//-----------------------------
		Map<String, List<GlobalConfig>> result = Maps.newHashMap();
		for(String worker : dataDistribution.keySet()){
			JSONArray blocks = dataDistribution.getJSONArray(worker);
			result.put(worker, new ArrayList<GlobalConfig>());
			for(int i = 0 ; i < blocks.size(); i++){
				int shardId = extractShardId(blocks.getString(i));
				GlobalConfig conf = GlobalConfig.build(shardId, algoName, algoConf, dataName, dataStatistic, workerAddresses, reducers, workerAddresses.get(shardId), configs.get(shardId));
				result.get(worker).add(conf);
			}
		}
		JSONObject r = new JSONObject();
		r.putAll(result);
		return r;
			
	}
	
	
	public static void main(String[] args) throws IOException {
		JSONObject dataDistribution = new JSONObject();
		dataDistribution.put("1.2.3.4:10000", Lists.asList("aa.v.0.dat", "aa.v.1.dat", new String[]{}));
		dataDistribution.put("1.2.3.4:10200", Lists.asList("aa.v.2.dat", "aa.v.3.dat", new String[]{}));
		
		JSONObject allocate = allocate(dataDistribution, "lr", DefaultAlgoConf.basicAlgoConf(10), "aa", FileUtil.loadProperties("leader_repo/data/real-sim/real-sim.sta"));
		
		System.out.println(allocate);
	}
	
}
