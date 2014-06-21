package org.shanbo.feluca.node.task;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.job.FelucaSubJob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * for test
 * @author lgn
 *
 */
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
	public String getTaskName() {
		return "dsleep";
	}

	@Override
	protected JSONArray distribTypeSubJob(JSONObject global) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();// all worker
		for(String worker : ClusterUtil.getWorkerList()){
			JSONObject conf = getDefaultConf(false);
			conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);
			JSONObject param  = global.getJSONObject("param");
			if (param != null)
				conf.getJSONObject("param").putAll(param); //using user-def's parameter
			
			concurrentLevel.add(conf);
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}
	
}
