package org.shanbo.feluca.node.leader.request;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.shanbo.feluca.node.request.BasicRequest;

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
		String jobType = request.param(RoleModule.JOB_TYPE, RoleModule.JOB_LOCAL);
		LeaderModule m = ((LeaderModule)module);
		if (jobName == null ){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobName'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		
		}else if (!jobType.equalsIgnoreCase(RoleModule.JOB_LOCAL) || !jobType.equalsIgnoreCase(RoleModule.JOB_DISTRIB)){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobType' == 'local' OR 'distrib'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			if (jobType.equalsIgnoreCase("local"))
				HttpResponseUtil.setResponse(resp, "kill job [" + jobName + "]", m.killJob(jobName, true));
			else {
				HttpResponseUtil.setResponse(resp, "kill job [" + jobName + "]", m.killJob(jobName, false));
			}
		}
	
		
	}

}
