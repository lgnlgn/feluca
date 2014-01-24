package org.shanbo.feluca.node.request;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.shanbo.feluca.node.worker.WorkerModule;

import com.alibaba.fastjson.JSONObject;

public class WorkerJobRequest extends BasicRequest{

	public static final String PATH = "/job";

	public WorkerJobRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}


	private void handleInfoRequest(NettyHttpRequest req, DefaultHttpResponse resp){
		String jobName = req.param("jobName");
		WorkerModule m = ((WorkerModule)module);

		if (jobName != null){
			JSONObject searchJobInfo = m.searchJobInfo(jobName);
			if (searchJobInfo == null){
				HttpResponseUtil.setResponse(resp, " query job :" + jobName, "null");
			}else{
				HttpResponseUtil.setResponse(resp, " query job :" + jobName, searchJobInfo);
			}
		}else{
			HttpResponseUtil.setResponse(resp, "feluca job status", m.getLatestJobStates());
		}

	}


	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String action = req.param("action","info");
		String contentJson = req.contentAsString();
		JSONObject conf = JSONObject.parseObject(contentJson);
		WorkerModule m = (WorkerModule)module;
		if (action.equals("submit")){
			try {
				String taskName =  m.submitJob(FelucaJob.class, conf);
				if (taskName != null)
					HttpResponseUtil.setResponse(resp, "submit task",taskName);
				else {
					HttpResponseUtil.setResponse(resp, "submit task", "failed");
				}
			} catch (Exception e) {
				HttpResponseUtil.setExceptionResponse(resp, "submit task", "failed", e);
			}
		}else if (action.equals("kill")){
			String taskName = req.param("jobName");
			if (taskName == null){
				HttpResponseUtil.setResponse(resp, "kill task ", "null");
			}else{
				HttpResponseUtil.setResponse(resp, "kill task : " + taskName, m.killJob(taskName));
			}
		}else if (action.equals("info")){
			handleInfoRequest(req, resp);
		}
	}

}
