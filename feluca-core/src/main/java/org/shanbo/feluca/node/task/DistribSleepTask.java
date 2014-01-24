package org.shanbo.feluca.node.task;

import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.util.ClusterUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class DistribSleepTask extends LocalSleepTask{

	final static int DEFAULT_SLEEP = 30000;
	
	public DistribSleepTask(JSONObject conf) {
		super(conf);
	}

	protected void init(JSONObject conf){
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP) == null?DEFAULT_SLEEP: conf.getJSONObject("param").getInteger(SLEEP);
	}

	@Override
	public boolean isLocalJob() {
		return false;
	}

	@Override
	public JSONArray arrangeSubJob(JSONObject global) {
		if ("local".equalsIgnoreCase(global.getString("type"))){ 
			//become local sleep
			JSONArray subJobSteps = new JSONArray(1);//only 1 step 
			JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
			JSONObject conf = reformNewConf(true);
			conf.getJSONObject("param").put(SLEEP, DEFAULT_SLEEP);
			JSONObject param  = global.getJSONObject("param");
			if (param != null)
				conf.getJSONObject("param").putAll(param); //using user-def's parameter
			
			concurrentLevel.add(conf);
			subJobSteps.add(concurrentLevel);
			return subJobSteps;
		}else{
			JSONArray subJobSteps = new JSONArray(1);//only 1 step 
			JSONArray concurrentLevel = new JSONArray();// all worker
			for(String worker : ClusterUtil.getWorkerList()){
				JSONObject conf = reformNewConf(false);
				conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);
				conf.getJSONObject("param").put(SLEEP, DEFAULT_SLEEP);
				JSONObject param  = global.getJSONObject("param");
				if (param != null)
					conf.getJSONObject("param").putAll(param); //using user-def's parameter
				
				concurrentLevel.add(conf);
			}
			subJobSteps.add(concurrentLevel);
			return subJobSteps;
		}

	}

	@Override
	public String getTaskName() {
		return "dsleep";
	}

}
