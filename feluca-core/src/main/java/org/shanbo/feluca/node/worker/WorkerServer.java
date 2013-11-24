package org.shanbo.feluca.node.worker;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.datasys.DataServer;
import org.shanbo.feluca.datasys.ftp.DataFtpServer;
import org.shanbo.feluca.util.ZKClient;

public class WorkerServer extends Server{
	WorkerModule module;
	DataServer dataServer;
	
	@Override
	public String serverName() {
		return "feluca.worker";
	}

	@Override
	public int defaultPort() {
		return 12030;
	}

	@Override
	public String zkRegisterPath() {
		return Constants.ZK_WORKER_PATH;
	}

	@Override
	public void preStart() throws Exception {
		
		ZKClient.get().createIfNotExist(Constants.ZK_CHROOT);
		ZKClient.get().createIfNotExist(zkRegisterPath() );
		module = new WorkerModule();
		module.init(zkRegisterPath(), getServerAddress());
		
		ZKClient.get().createIfNotExist(Constants.FDFS_ZK_ROOT);
		dataServer = new DataFtpServer();
		dataServer.start();

	}

	@Override
	public void postStop() throws Exception {
		module.shutdown();
		dataServer.stop();
		
	}
	public static void main(String[] args) {
		WorkerServer server = new WorkerServer();
		server.start();
	}
}
