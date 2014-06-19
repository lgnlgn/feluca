package org.shanbo.feluca.node.request;


import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.JobManager;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.job.FelucaJob;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.shanbo.feluca.util.DateUtil;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.Strings;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;

public class LeaderJobRequest extends BasicRequest{

	public LeaderJobRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		return "/job";
	}

	private void handleInfoRequest(NettyHttpRequest req, DefaultHttpResponse resp){
		String numJobs = req.param("last", "5"); //default 5
		String jobType = req.param("isLocal", "true");
		String jobName = req.param("jobName");
		LeaderModule m = ((LeaderModule)module);
		if (!jobType.equalsIgnoreCase("true") && !jobType.equalsIgnoreCase("false")){
			HttpResponseUtil.setResponse(resp, "info action", "require 'isLocal' == 'true' OR 'false'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			boolean isLocal = new Boolean(jobType.toLowerCase());
			if (jobName != null){
				JSONObject searchJobInfo = m.searchJobInfo(jobName, isLocal);
				if (searchJobInfo == null){
					HttpResponseUtil.setResponse(resp, " query job :" + jobName, "null");
				}else{
					HttpResponseUtil.setResponse(resp, " query job :" + jobName, searchJobInfo);
				}
			}else if (numJobs != null){
				int num = new Integer(numJobs);
				HttpResponseUtil.setResponse(resp, "latest jobs", m.getLatestJobStates(num,isLocal));
			}else{
				HttpResponseUtil.setResponse(resp, "feluca job status", m.getLatestJobStates(1,isLocal));
			}
		}
	}

	private  void handleJobSubmit(NettyHttpRequest req, DefaultHttpResponse resp){
		LeaderModule m = (LeaderModule)this.module;
		String content = req.contentAsString();
		JSONObject parameters = new JSONObject();
		
		if (StringUtils.isBlank(content)){
			HttpResponseUtil.setResponse(resp, "submitJob failed! you need to post content : currently we have tasks:", FelucaJob.getTaskList());
			return;
		}
		
		try{
			parameters.putAll(JSONObject.parseObject(content));
			String submitJob = m.submitJob(FelucaJob.class, parameters);
			if (submitJob == null){
				HttpResponseUtil.setResponse(resp, "submitJob", "failed!");
			}else{
				HttpResponseUtil.setResponse(resp, "submitJob", submitJob);
			}
		} catch (Exception e) {
			HttpResponseUtil.setExceptionResponse(resp, Strings.keyValuesToJson("action", "submitJob"), 
					"submit error", e);
		}
	}


	private void handleJobKill(NettyHttpRequest req, DefaultHttpResponse resp) {
		String jobName = req.param("jobName"); //default 5
		LeaderModule m = ((LeaderModule)module);
		if (jobName == null ){
			HttpResponseUtil.setResponse(resp, "kill job action", "require 'jobName'");
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
		}else{
			JSONObject result = Strings.keyValuesToJson("local job",  m.killJob(jobName, true), "distrib job" ,  m.killJob(jobName, false));
			HttpResponseUtil.setResponse(resp, "kill job [" + jobName + "]", result);
		}

	}

	public void displayHelpInfo(NettyHttpRequest req, DefaultHttpResponse resp){
		HttpResponseUtil.setResponse(resp, "'action' parameters", JSONUtil.fromStrings("info", "submit", "kill"));
	}
	
	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String action = req.param("action","help");
		if (action.equalsIgnoreCase("submit")){
			this.handleJobSubmit(req, resp);
		}else if (action.equalsIgnoreCase("info")) {
			this.handleInfoRequest(req, resp);
		}else if (action.equalsIgnoreCase("kill")) {
			this.handleJobKill(req, resp);
		}else{
			this.displayHelpInfo(req, resp);
		}
	}

}
