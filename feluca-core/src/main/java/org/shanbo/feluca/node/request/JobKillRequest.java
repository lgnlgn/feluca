package org.shanbo.feluca.node.request;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JobKillRequest extends BasicRequest{

	public static String PATH = "/kill";
	
	public JobKillRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest request, DefaultHttpResponse resp) {
		String jobName = request.param("jobName"); //default 5
		LeaderModule m = ((LeaderModule)module);
		if (jobName == null){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobName'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			;
			HttpResponseUtil.setResponse(resp, "kill job [" + jobName + "]", m.killJob(jobName));
		}
	
		
	}

}
