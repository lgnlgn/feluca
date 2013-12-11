package org.shanbo.feluca.node.request;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;










import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author lgn-mop
 *
 */
public class JobStatusHandler extends BasicRequest{

	public final static String PATH = "/jobstatus";
	
	public JobStatusHandler(LeaderModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest request, DefaultHttpResponse resp) {
		String jobs = request.param("last"); //default 5
		String jobName = request.param("name");
		LeaderModule m = ((LeaderModule)module);
		if (jobName != null){
			JSONObject searchJobInfo = m.searchJobInfo(jobName);
			if (searchJobInfo == null){
				HttpResponseUtil.setResponse(resp, " query job :" + jobName, JobManager.JOB_NOT_FOUND);
			}else{
				HttpResponseUtil.setResponse(resp, " query job :" + jobName, searchJobInfo);
			}
		}else if (jobs != null){
			int num = new Integer(jobs);
			HttpResponseUtil.setResponse(resp, "latest jobs", m.getLatestJobStates(num));
		}else{
			String jobString = m.getJobStatus();
			HttpResponseUtil.setResponse(resp, "feluca job status", jobString);
		}
	}

}
