package org.shanbo.feluca.node.leader.request;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;












import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.shanbo.feluca.node.request.BasicRequest;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author lgn-mop
 *
 */
public class JobStatusRequest extends BasicRequest{

	public final static String PATH = "/jobstatus";
	
	public JobStatusRequest(LeaderModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest request, DefaultHttpResponse resp) {
		String jobs = request.param("last"); //default 5
		String jobType = request.param(RoleModule.JOB_TYPE, RoleModule.JOB_LOCAL);
		String jobName = request.param("name");
		LeaderModule m = ((LeaderModule)module);
		if (!jobType.equalsIgnoreCase(RoleModule.JOB_LOCAL) || !jobType.equalsIgnoreCase(RoleModule.JOB_DISTRIB)){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobType' == 'local' OR 'distrib'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			boolean isLocal = jobType.equalsIgnoreCase("local")?true:false;
			if (jobName != null){
				JSONObject searchJobInfo = m.searchJobInfo(jobName, isLocal);
				if (searchJobInfo == null){
					HttpResponseUtil.setResponse(resp, " query job :" + jobName, JobManager.JOB_NOT_FOUND);
				}else{
					HttpResponseUtil.setResponse(resp, " query job :" + jobName, searchJobInfo);
				}
			}else if (jobs != null){
				int num = new Integer(jobs);
				HttpResponseUtil.setResponse(resp, "latest jobs", m.getLatestJobStates(num,isLocal));
			}else{
				String jobString = m.getLatestJobStates(1,isLocal).toJSONString();
				HttpResponseUtil.setResponse(resp, "feluca job status", jobString);
			}
		}
	}

}
