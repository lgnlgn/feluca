package org.shanbo.feluca.node;


import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.http.Handler;

/**
 * leader handler 
 * worker handler 
 * @author shanbo.liang
 *
 */
public abstract class RequestHandler implements Handler{
	
	protected RoleModule module;
	// for distributed request
	
	public RequestHandler(RoleModule module){
		this.module = module;

	}
}
