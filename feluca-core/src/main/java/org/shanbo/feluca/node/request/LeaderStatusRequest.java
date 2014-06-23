package org.shanbo.feluca.node.request;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderModule;

import com.alibaba.fastjson.JSONObject;

public class LeaderStatusRequest extends BasicRequest{

	public LeaderStatusRequest(RoleModule module) {
		super(module);
		// TODO Auto-generated constructor stub
	}

	public String getPath() {
		return "/state";
	}

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		String type = req.param("type");
		if (type.equalsIgnoreCase("cluster")){
			String leaderAddress = module.getModuleAddress();
			List<String> workers = ClusterUtil.getWorkerList();
			JSONObject json = new JSONObject();
			json.put("leader", leaderAddress);
			json.put("worker", workers);
			HttpResponseUtil.setResponse(resp, "cluster status", json);
		}else if(type.equalsIgnoreCase("localdata")){
			//TODO
		}else if (type.equalsIgnoreCase("distribdata")){
			//TODO
			String dataName = req.param("name");
			if (StringUtils.isNotBlank(dataName)){
				
			}else{
				
			}
		}
		
	}

}
