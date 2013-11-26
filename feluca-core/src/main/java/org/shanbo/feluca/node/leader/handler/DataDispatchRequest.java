package org.shanbo.feluca.node.leader.handler;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.RequestHandler;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.NettyHttpRequest;

public class DataDispatchRequest extends RequestHandler{

	public static String PATH = "/dataSend";
	
	public DataDispatchRequest(RoleModule module) {
		super(module);
	}

	public String getPath() {
		return PATH;
	}

	public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
		// TODO Auto-generated method stub
		
	}

}
