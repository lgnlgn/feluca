package org.shanbo.feluca.node.request;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONArray;
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
		List<String> workers = ClusterUtil.getWorkerList();
		if (type.equalsIgnoreCase("cluster")){
			String leaderAddress = module.getModuleAddress();
			JSONObject json = new JSONObject();
			json.put("leader", leaderAddress);
			json.put("worker", workers);
			HttpResponseUtil.setResponse(resp, "cluster status", json);
		}else if(type.equalsIgnoreCase("localdata")){
			//TODO
		}else if (type.equalsIgnoreCase("distribdata")){
			//TODO more robust
			String dataName = req.param("dataName");
			if (StringUtils.isBlank(dataName)){
				//merge all workers dataSets
				try {
					Map<String, String> result = HttpClientUtil.distribGet(workers, "/state?type=data");
					//TODO  intact checking
					HashSet<String> dataSets = new HashSet<String>();
					for(String responseString : result.values()){
						JSONObject jo = JSONObject.parseObject(responseString);
						dataSets.addAll(JSONUtil.JSONArrayToList(jo.getJSONArray("response")));
					}
					HttpResponseUtil.setResponse(resp, "cluster data list", JSONUtil.listToAJsonArray(dataSets));
				} catch (Exception e) {
					HttpResponseUtil.setExceptionResponse(resp, "fetch cluster data list", "cause exception", e);
				}
			}else{
				try {
					Map<String, String> result = HttpClientUtil.distribGet(workers, "/state?type=data?dataName="+ dataName);
					//TODO  intact checking
					JSONObject invert = new JSONObject();
					for(Entry<String, String> addressAndBlocks: result.entrySet()){
						JSONArray blocks = JSONObject.parseObject(addressAndBlocks.getValue()).getJSONArray("response");
						for(Object block : blocks){
							JSONArray addresses = invert.getJSONArray(block.toString());
							if (addresses == null){
								addresses = new JSONArray();
								invert.put(block.toString(), addresses);
							}
							addresses.add(addressAndBlocks.getKey());
						}
					}
					HttpResponseUtil.setResponse(resp, "cluster data list", invert);
				} catch (Exception e) {
					HttpResponseUtil.setExceptionResponse(resp, "fetch cluster dataset : [" + dataName + "]", "cause exception", e);
				}
			}
		}
		
	}

}
