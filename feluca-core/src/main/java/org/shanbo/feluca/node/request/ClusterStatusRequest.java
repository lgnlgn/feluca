package org.shanbo.feluca.node.request;

import java.util.List;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;

import com.alibaba.fastjson.JSONObject;
/**
 * 
 *  @Description check cluster status
 *	@author shanbo.liang
 */
public class ClusterStatusRequest extends BasicRequest{

	public final static String PATH = "/cluster";
	
	public ClusterStatusRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String leaderAddress = module.getModuleAddress();
		List<String> slaves = ((LeaderModule)module).yieldSlaves();
		JSONObject json = new JSONObject();
		json.put("leader", leaderAddress);
		json.put("worker", slaves);
		HttpResponseUtil.setResponse(resp, "cluster status", json);
	}
	
}
