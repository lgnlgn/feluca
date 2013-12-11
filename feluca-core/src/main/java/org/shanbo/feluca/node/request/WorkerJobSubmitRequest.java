package org.shanbo.feluca.node.request;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.FelucaJob;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.worker.WorkerModule;
import org.shanbo.feluca.node.worker.job.PullDataJob;

import com.alibaba.fastjson.JSONObject;

public class WorkerJobSubmitRequest extends BasicRequest{

	public static final String PATH = "/job";
	
	private Map<String, Class<? extends FelucaJob>> jobClassMap = new HashMap<String, Class<? extends FelucaJob>>();
	
	public WorkerJobSubmitRequest(RoleModule module) {
		super(module);
		jobClassMap.put("pullData", PullDataJob.class);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String taskName = req.param("taskName");
		Class<? extends FelucaJob> clz = jobClassMap.get(taskName);
		if (clz == null){
			HttpResponseUtil.setResponse(resp, "assign job:" + taskName, "no such job can be runned", HttpResponseStatus.BAD_REQUEST);
		}else{
			String contentJson = req.contentAsString();
			JSONObject conf = JSONObject.parseObject(contentJson);
			WorkerModule m = (WorkerModule)module;
			try {
				m.submitJob(clz, conf);
			} catch (Exception e) {
				
			}
		}
	}

}
