package org.shanbo.feluca.node.job.local;

import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * 
 * @author lgn
 *
 */
public class LocalDeleteJob extends SubJobAllocator{

	@Override
	public JSONArray allocateSubJobs(JSONObject udconf) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject taskTicket = getTask("filedelte").taskSerialize("filedelete");
		JSONObject param  = udconf.getJSONObject("param"); //get user-def parameters
		if (param != null){
			taskTicket.getJSONObject("param").putAll(param); //using user-def's parameter
		}
		concurrentLevel.add(taskTicket);
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getName() {
		return "filedelete";
	}

}
