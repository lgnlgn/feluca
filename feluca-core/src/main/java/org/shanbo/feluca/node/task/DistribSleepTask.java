package org.shanbo.feluca.node.task;

import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.util.ClusterUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class DistribSleepTask extends TestLocalSleepTask{

	
	
	public DistribSleepTask(JSONObject conf) {
		super(conf);
	}

	protected void init(JSONObject conf){
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP) == null?10000: conf.getJSONObject("param").getInteger(SLEEP);
	}

	@Override
	protected boolean isLocalJob() {
		return false;
	}

	@Override
	public JSONArray parseConfForJob(JSONObject param) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();// all worker
		for(String worker : ClusterUtil.getWorkerList()){
			JSONObject conf = baseConfTemplate(false);
			conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);
			conf.getJSONObject("param").put(SLEEP, "30000");
			if (param != null)
				conf.getJSONObject("param").putAll(param); //using user-def's parameter
			conf.put("task", this.getClass().getName()); //do not forget
			concurrentLevel.add(conf);
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;

	}

	@Override
	public String getTaskName() {
		return "dsleep";
	}

}
