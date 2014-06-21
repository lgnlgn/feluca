package org.shanbo.feluca.node.task;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 *  @Description TODO
 *	@author shanbo.liang
 */
public class LocalMultiSleepTask extends LocalSleepTask{


	public LocalMultiSleepTask(JSONObject conf) {
		super(conf);
	}
	protected void init(JSONObject conf){
		JSONObject param = conf.getJSONObject("param");
		if (param == null){
			sleepMs = 5000;
		}else{
			sleepMs = param.getInteger(SLEEP) == null?10000: param.getInteger(SLEEP);
		}

	}

	@Override
	public JSONArray arrangeSubJob(JSONObject global) {
		JSONObject param = global.getJSONObject("param");
		int NUM_SLEEP = 2;
		int CONCURRENT_SLEEP = 2;
		if (param!= null){
			NUM_SLEEP = param.getInteger("loop") == null?2: param.getInteger("loop");
			CONCURRENT_SLEEP = param.getInteger("thread") == null?2: param.getInteger("thread");
		}
		JSONArray subJobSteps = new JSONArray(NUM_SLEEP);//multiple sleeps 

		for(int i = 0 ; i < NUM_SLEEP; i++){
			JSONArray concurrentLevel = new JSONArray(CONCURRENT_SLEEP);// needs multiple threads 
			for(int j = 0; j < CONCURRENT_SLEEP; j++){
				JSONObject conf = getDefaultConf(true);
				conf.getJSONObject("param").put(SLEEP, sleepMs);
				if (param != null){
					conf.getJSONObject("param").putAll(param); //using user-def's parameter
				}
				concurrentLevel.add(conf);
			}
			subJobSteps.add(concurrentLevel);
		}
		return subJobSteps;
	}

	@Override
	public final String getTaskName() {
		return "msleep";
	}

}
