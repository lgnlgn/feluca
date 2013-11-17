package org.shanbo.feluca.datasys;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;

public abstract  class DataServer extends Server{
	
	public final String zkRegisterPath(){
		return Constants.FDFS_ZK_ROOT;
	}
	
	public final String serverName(){
		return Constants.FDFS_SERVER_NAME;
	}
	
	public int defaultPort() {
		return 12221;
	}
}
