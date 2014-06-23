package org.shanbo.feluca.node.request;


import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.node.http.Handler;

/**
 * leader handler 
 * worker handler 
 * @author shanbo.liang
 *
 */
public abstract class BasicRequest implements Handler{
	
	protected RoleModule module;
	// for distributed request
	
	public BasicRequest(RoleModule module){
		this.module = module;

	}
}
