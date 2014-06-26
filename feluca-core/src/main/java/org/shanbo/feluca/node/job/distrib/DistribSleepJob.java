package org.shanbo.feluca.node.job.distrib;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * just for test
 * @author lgn
 *
 */
public class DistribSleepJob extends SubJobAllocator{

	@Override
	public JSONArray allocateSubJobs(JSONObject udConf) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();     // all workers
		for(String worker : ClusterUtil.getWorkerList()){// all workers
			JSONObject conf = TASKS.get("lsleep").taskSerialize();
			conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker); //more
			JSONObject param  = udConf.getJSONObject("param");
			if (param != null)
				conf.getJSONObject("param").putAll(param); //using user-def's parameter
			
			concurrentLevel.add(conf);
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getName() {
		return "dsleep";
	}

}
