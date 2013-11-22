package org.shanbo.feluca.node.leader.handler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;








import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.RequestHandler;
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
public class JobStatusHandler extends RequestHandler{

	public final static String PATH = "/jobstatus";
	
	public JobStatusHandler(LeaderModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest request, DefaultHttpResponse resp) {
		String jobs = request.param("last"); //default 5
		LeaderModule m = ((LeaderModule)module);
		if (jobs != null){
			int num = new Integer(jobs);
			HttpResponseUtil.setResponse(resp, "latest jobs", m.getLatestJobStatus(num));
		}else{
			JSONObject json = new JSONObject();
			JSONArray ja = new JSONArray();
			ja.addAll(m.yieldSlaves());
			json.put("live_slaves", ja);
			String jobString = m.getJobStatus();
			if (jobString.startsWith("{") && jobString.endsWith("}"))
				json.put("running_job", JSONObject.parse(jobString));
			else
				json.put("running_job", jobString);
			HttpResponseUtil.setResponse(resp, "feluca job status", json.toString());
		}
	}

}
