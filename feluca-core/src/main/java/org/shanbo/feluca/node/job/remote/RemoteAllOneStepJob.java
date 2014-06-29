package org.shanbo.feluca.node.job.remote;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class RemoteAllOneStepJob extends SubJobAllocator{

	private String jobName;
	private String taskName;

	public RemoteAllOneStepJob(String jobName, String taskName){
		this.jobName = jobName;
		this.taskName = taskName;
	}
	@Override
	public JSONArray allocateSubJobs(JSONObject udConf) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();     // all workers

		for(String worker : ClusterUtil.getWorkerList()){// all workers
			JSONObject conf = getTaskTicket(taskName); //distribute sleep -> local sleep
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
	public String getJobName() {
		return jobName;
	}

}
