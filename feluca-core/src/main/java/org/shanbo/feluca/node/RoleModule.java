package org.shanbo.feluca.node;

import org.shanbo.feluca.util.ZKClient;



/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class RoleModule {
	
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
