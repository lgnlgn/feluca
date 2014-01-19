package org.shanbo.feluca.node.request;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.job.FelucaJob;
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

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String action = req.param("action","status");


		String contentJson = req.contentAsString();
		JSONObject conf = JSONObject.parseObject(contentJson);
		WorkerModule m = (WorkerModule)module;
		if (action.equals("submit")){
			try {
				String taskName =  m.submitJob(FelucaJob.class, conf);
				HttpResponseUtil.setResponse(resp, "submit task",taskName);
			} catch (Exception e) {
				HttpResponseUtil.setResponse(resp, "submit task",null);
			}
		}else if (action.equals("kill")){
			String taskName = req.param("taskName");
			if (taskName == null){
				HttpResponseUtil.setResponse(resp, "kill task", JobManager.JOB_NOT_FOUND);
			}else{
				m.killJob(taskName);
				HttpResponseUtil.setResponse(resp, "kill task : " + taskName,"action submitted!");
			}
		}else if (action.equals("info")){
			String taskName = req.param("taskName");
			if (taskName == null){
				HttpResponseUtil.setResponse(resp, "task status",JobManager.JOB_NOT_FOUND);
			}else{
				HttpResponseUtil.setResponse(resp, "task status : "+ taskName, m.searchJobInfo(taskName) );
			}
		}
	}

}
