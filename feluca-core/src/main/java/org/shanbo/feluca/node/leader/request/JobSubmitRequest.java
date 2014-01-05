package org.shanbo.feluca.node.leader.request;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


import java.util.Properties;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.shanbo.feluca.node.leader.job.DataDispatchJob;
import org.shanbo.feluca.node.leader.job.StoppableSleepJob;
import org.shanbo.feluca.node.request.BasicRequest;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/**
 * 
 * @author shanbo.liang
 *
 */
public class JobSubmitRequest extends BasicRequest{
	
	static Logger log = LoggerFactory.getLogger(JobSubmitRequest.class);
	
	public static final String PATH = "/job";
	public JobSubmitRequest(RoleModule module) {
		super(module);
	}

	

	final static List<String> jobAllow = new ArrayList<String>();
	static {
		jobAllow.add("data");
//		jobAllow.add("algorithm");
		jobAllow.add("sleep");
	}
	
	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest request, DefaultHttpResponse resp) {
		String jobClass = request.param("jobClass");
		String jobType = request.param(RoleModule.JOB_TYPE, RoleModule.JOB_LOCAL);
		LeaderModule m = (LeaderModule)this.module;

		if (!jobType.equalsIgnoreCase(RoleModule.JOB_LOCAL) || !jobType.equalsIgnoreCase(RoleModule.JOB_DISTRIB)){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobType' == 'local' OR 'distrib'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			boolean isLocal = jobType.equalsIgnoreCase("local")?true:false;
			JSONObject parameters = new JSONObject();
			parameters.put("jobClass", jobClass);
			String jobName = jobClass + "_" + DateUtil.getMsDateTimeFormat();
			parameters.put("jobName", jobName);
			try {
				String submitJob = m.submitJob(FelucaJob.class, parameters, isLocal);
				HttpResponseUtil.setResponse(resp, 
						Strings.keyValuesToJson("action", "submitJob", "isLocal", isLocal), 
						Strings.keyValuesToJson("jobName", submitJob, "isLocal", isLocal));
			} catch (Exception e) {
				HttpResponseUtil.setExceptionResponse(resp, Strings.keyValuesToJson("action", "submitJob", "isLocal", isLocal), 
						"submit error", e);
			}
			
			
		}
		
		
	}

}
