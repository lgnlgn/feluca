package org.shanbo.feluca.node;


import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.http.Handler;

/**
 * master handler 
 * slave handler 
 * @author lgn-mop
 *
 */
public abstract class RequestHandler implements Handler{
	
	protected RoleModule module;
	// for distributed request
	
	public RequestHandler(RoleModule module){
		this.module = module;

	}
}
