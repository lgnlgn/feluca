package org.shanbo.feluca.common;

import org.shanbo.feluca.util.Config;


public class Constants {
	
	public static class Base{
		public final static String DATA_DIR = "./data";
		

		public final static String ZK_CHROOT = "/feluca";
		
		public final static String FDFS_ZK_ROOT = ZK_CHROOT + "/fdfs";
		
		public final static String FDFS_SERVER_NAME = "FDSFServer";
		
		public final static String DATA_PATH = Config.get().get("datadir", "./data");
		public final static String WORKER_DATA_PATH = Config.get().get("workerdatadir", "./data2");

		public final static String ZK_WORKER_PATH = ZK_CHROOT + "/workers";
		public final static String ZK_LEADER_PATH = ZK_CHROOT + "/leader";
		
	}
	
	
	public static class Algorithm{
		
	}
	
	
	public static class Network{
		public final static String leaderToWorkerText="feluca";		
	}
	
}
