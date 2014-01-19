package org.shanbo.feluca.node.request;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.worker.WorkerModule;

public class WorkerStatusRequest extends BasicRequest{

	public WorkerStatusRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		// TODO Auto-generated method stub
		return "/state";
	}

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		WorkerModule m = (WorkerModule)module;
		String type = req.param("type");
		if (type.equals("data")){
			String dataName = req.param("dataName");
			if (dataName == null){
				HttpResponseUtil.setResponse(resp, "show data list", m.listDataSets());
			}else{
				HttpResponseUtil.setResponse(resp, "show data : " + dataName, m.listDataBlocks(dataName));
			}
		}
		
	}

}
