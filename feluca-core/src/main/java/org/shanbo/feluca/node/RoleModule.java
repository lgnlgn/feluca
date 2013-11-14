package org.shanbo.feluca.node;

import org.shanbo.feluca.util.ZKClient;



/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public abstract class RoleModule {
	
	private String address;
	
	public void register(String path, String address) throws Exception{
		ZKClient.get().registerEphemeralNode(path, address);
		this.address = address;
	}
	
	public String getModuleAddress(){
		return address;
	}
}
