package org.shanbo.feluca.node.job.distrib;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.SubJobAllocator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class RemoteDeleteJob extends SubJobAllocator{
	final static String TaskName = "filedelete";

	@Override
	public JSONArray allocateSubJobs(JSONObject udConf) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray();     // corresponding workers
		if (FelucaSubJob.isSubJobLocal(udConf)){
			concurrentLevel.add(udConf); //received ticket(modified by FelucaSubJob); directly use it
		}else{
			JSONArray toDeleteFiles = udConf.getJSONObject("param").getJSONArray("files");
			if (toDeleteFiles != null ){ //tell all workers do same things
				for(String worker : ClusterUtil.getWorkerList()){// all workers
					JSONObject conf = getTaskTicket(TaskName);
					conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker);
					conf.getJSONObject("param").put("files", toDeleteFiles);
					concurrentLevel.add(conf);
				}
			}else{
				//TODO specify detail delete actions, maybe useless ; needs more robust
				JSONObject workerDetail = udConf.getJSONObject("param").getJSONObject("detail");
				if (workerDetail!=null){
					for(String hostPort : workerDetail.keySet()){
						JSONArray workerToDeleteFiles = workerDetail.getJSONArray(hostPort);
						JSONObject conf = getTaskTicket(TaskName);
						conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, hostPort);
						conf.getJSONObject("param").put("files", workerToDeleteFiles);
						concurrentLevel.add(conf);
					}
				}else{
					throw new FelucaException("not found 'files' OR 'detail' in the POST BODY");
				}
			}
		}
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getName() {
		return "ddelete";
	}

}
