package org.shanbo.feluca.node.job.distrib;

import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * simple process: 
 * <p>1. delete old from all workers(TODO more intelligent)
 * <p>2. allocate blocks & send corresponding pull action to all workers
 * @author lgn
 *
 */
public class DataBalanceJob extends SubJobAllocator{

	@Override
	public JSONArray allocateSubJobs(JSONObject properties) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		return "databalance";
	}

}
