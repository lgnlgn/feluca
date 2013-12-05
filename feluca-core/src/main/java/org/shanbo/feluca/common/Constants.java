package org.shanbo.feluca.common;

import org.shanbo.feluca.util.Config;


public class Constants {
	
	public static class Base{
		public final static String LEADER_REPOSITORY =  Config.get().get("datadir", "./leader_repo");
		public final static String WORKER_REPOSITORY =  Config.get().get("datadir", "./worder_repo");
		
		public final static String LEADER_DATASET_DIR = LEADER_REPOSITORY + "/data";
		public final static String WORKER_DATASET_DIR = WORKER_REPOSITORY + "/data";
		
		public final static String ZK_CHROOT = "/feluca";
		public final static String ZK_WORKER_PATH = ZK_CHROOT + "/workers";
		public final static String ZK_LEADER_PATH = ZK_CHROOT + "/leader";
		public final static String FDFS_ZK_ROOT = ZK_CHROOT + "/fdfs";
		
		public final static String FDFS_SERVER_NAME = "FDSFServer";
				
	}
	
	
	public static class Algorithm{
		
	}
	
	
	public static class Network{
		public final static String leaderToWorkerText="feluca";		
	}
	
}
