package org.shanbo.feluca.common;

import org.shanbo.feluca.util.Config;


public class Constants {
	private static String LEADER_REPOSITORY ;
	private static String WORKER_REPOSITORY ;
	static{
		String leaderRepo = Config.get().get("leader.repo");
		if (leaderRepo == null){
			LEADER_REPOSITORY = ClusterUtil.getProperties("leader.repo", "./leader_repo");
		}else{
			LEADER_REPOSITORY = leaderRepo;
		}
		String workerRepo = Config.get().get("leader.repo");
		if (workerRepo == null){
			WORKER_REPOSITORY = ClusterUtil.getProperties("worker.repo", "./worker_repo");
		}else{
			WORKER_REPOSITORY = workerRepo;
		}
		
	}
	
	public static class Base{
		
		public static String getLeaderRepository(){
			return Constants.LEADER_REPOSITORY;
		}
		
		public static String getWorkerRepository(){
			return Constants.WORKER_REPOSITORY;
		}
		
		public final static String DATA_DIR = "/data";
		
		public final static String LEADER_DATASET_DIR = LEADER_REPOSITORY + DATA_DIR;
		public final static String WORKER_DATASET_DIR = WORKER_REPOSITORY + DATA_DIR;
		
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
