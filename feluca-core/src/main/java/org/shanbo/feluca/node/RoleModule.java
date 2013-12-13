package org.shanbo.feluca.node;

import org.shanbo.feluca.util.ZKClient;



/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class RoleModule {

	public final static String JOB_TYPE = "jobType";
	public final static String JOB_LOCAL = "local";
	public final static String JOB_DISTRIB = "distrib";
	
	private String address;
	private String moduleEphemeralNode;

	
	public void init(String path, String address) throws Exception{
		this.moduleEphemeralNode = path;
		this.address = address;
		ZKClient.get().registerEphemeralNode(path, address);
	}
	
	public String getModuleAddress(){
		return address;
		
	}
	
	public void shutdown() throws Exception{
		ZKClient.get().unRegisterEphemeralNode(moduleEphemeralNode, address);
	}
}
