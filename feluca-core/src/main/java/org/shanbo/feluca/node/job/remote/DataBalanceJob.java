package org.shanbo.feluca.node.job.remote;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.node.job.FelucaSubJob;
import org.shanbo.feluca.node.job.SubJobAllocator;
import org.shanbo.feluca.util.FileUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.Utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

/**
 * simple process: 
 * <p>1. delete old from all workers(TODO more intelligent)
 * <p>2. allocate blocks & send corresponding pull action to all workers
 * TODO smartly check
 * @author lgn
 *
 */
public class DataBalanceJob extends SubJobAllocator{

	@Override
	public JSONArray allocateSubJobs(JSONObject udConf) {
		String dataName = udConf.getJSONObject("param").getString("dataName");
		if (dataName == null)
			throw new FelucaException("'dataName' is require in 'param'");

		JSONArray subJobSteps = new JSONArray(2);//2 steps: 1. delete & 2.pull 
		JSONArray deleteStep= new JSONArray();
		JSONArray pullStep= new JSONArray();
		try {
			List<String> workers = ClusterUtil.getWorkerList();
			//---------------step 1
			List<String> toDelete = ImmutableList.of(dataName);
			for(String worker : workers){
				JSONObject conf = getTaskTicket("filedelete"); //distribute sleep
				conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, worker); //more
				conf.getJSONObject("param").put("files", JSONUtil.listToAJsonArray(toDelete));
				deleteStep.add(conf);
			}
			//----------------step2
			Properties dataStatus = FileUtil.loadProperties(Constants.Base.getLeaderRepository() + Constants.Base.DATA_DIR + "/" + dataName + "/" + dataName + ".sta");
			int totalBlocks = Utils.getIntFromProperties(dataStatus, "totalBlocks");

			List<List<Integer>> allocationList = Utils.hashAllocate(totalBlocks, workers.size());
			for(int i = 0 ; i < allocationList.size(); i++ ){// all workers
				JSONObject conf = getTaskTicket("file"); //distribute sleep -> local sleep
				conf.put(FelucaSubJob.DISTRIBUTE_ADDRESS_KEY, workers.get(i)); //more
				JSONArray pullFiles = new JSONArray();
				for(int id : allocationList.get(i)){ //each block
					pullFiles.add(Constants.Base.DATA_DIR + "/" + dataName + "/" + dataName + "_" + id + ".sta");
					pullFiles.add(Constants.Base.DATA_DIR + "/" + dataName + "/" + dataName + "_" + id + ".dat");
				}
				pullFiles.add(Constants.Base.DATA_DIR + "/" + dataName + "/" + dataName + ".sta");//global statistic
				conf.getJSONObject("param").put("files", pullFiles);
				pullStep.add(conf);
			}

		} catch (IOException e) {
			throw new FelucaException("check dataset error", e);
		}
		subJobSteps.add(deleteStep);
		subJobSteps.add(pullStep);
		return subJobSteps;
	}

	@Override
	public String getJobName() {
		return "databalance";
	}

}
