package org.shanbo.feluca.node.leader.handler;

import java.util.ArrayList;
import java.util.List;


import java.util.Properties;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.node.RequestHandler;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.job.DataDispatchJob;
import org.shanbo.feluca.node.job.StoppableSleepJob;
import org.shanbo.feluca.node.leader.LeaderModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author shanbo.liang
 *
 */
public class JobSubmitRequest extends RequestHandler{
	
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

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String jobName = req.param("jobname");
		
		if (!jobAllow.contains(jobName)){
			HttpResponseUtil.setResponse(resp, "start job :" + jobName +" found by 'jobname'", " Only allowed:" + jobAllow);
			resp.setStatus(HttpResponseStatus.BAD_REQUEST);
			return;
		}
		LeaderModule m = (LeaderModule)this.module;
		Class<? extends FelucaJob> jobClz = null;
		Properties properties = null;
		if (jobName.equals("data")){
			String dataName = req.param("dataName");
			if (dataName == null){
				HttpResponseUtil.setResponse(resp, "start job : data", " require 'dataName' ");
				resp.setStatus(HttpResponseStatus.BAD_REQUEST);
				return;
			}else if (!m.dataSetExist(dataName)){
				HttpResponseUtil.setResponse(resp, "start job : data", " dataName:[" + dataName+ "] not found! ");
				resp.setStatus(HttpResponseStatus.BAD_REQUEST);
				return;
			}
			jobClz = DataDispatchJob.class;
		}else if (jobName.equals("sleep")){
			String ms = req.param("ms");
			jobClz = StoppableSleepJob.class;
			if (ms != null){
				properties = new Properties();
				properties.put("job.ttl", new Integer(ms));
			}
		}

		String submitJobName;
		try {
			submitJobName = m.submitJob(jobClz, properties);
			if (submitJobName != null){
				HttpResponseUtil.setResponse(resp, "start job : data", "job submited:" + submitJobName);
			}else{
				HttpResponseUtil.setResponse(resp, "start job : data", "submision failed, a job already running ");
			}
		} catch (Exception e) {
			log.error("submit job error", e);
			HttpResponseUtil.setResponse(resp, "start job : data", "submision failed  Class init error " );
		} 

	}

}
