package org.shanbo.feluca.node.job.distrib;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * send same file list to all workers
 * @author lgn
 *
 */
public class FileDistributeJob extends SubJobAllocator{

	final static String TaskName = "filepull";

	@Override
	public JSONArray allocateSubJobs(JSONObject udConf) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();     // all workers
		if (FelucaSubJob.isSubJobLocal(udConf)){
			concurrentLevel.add(udConf); //received ticket(modified by FelucaSubJob); directly use it
		}else{
			for(String worker : ClusterUtil.getWorkerList()){// all workers

				JSONObject conf = getTaskTicket(TaskName);
				conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);

				JSONObject param  = udConf.getJSONObject("param");
				if (param != null)
					conf.getJSONObject("param").putAll(param); //using user-def's parameter

				concurrentLevel.add(conf);
			}
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getName() {
		return "filepull";
	}


}
