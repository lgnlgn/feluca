package org.shanbo.feluca.common;

import org.shanbo.feluca.util.Config;


public class Constants {
	private static String LEADER_REPOSITORY ;
	private static String WORKER_REPOSITORY ;
	static{
		String leaderRepo = Config.get().get("leader.repo");
		LEADER_REPOSITORY = ClusterUtil.getProperties("leader.repo", "./leader_repo");
		if (leaderRepo != null){
			LEADER_REPOSITORY = leaderRepo;
		}
		String workerRepo = Config.get().get("leader.repo");
		WORKER_REPOSITORY = ClusterUtil.getProperties("worker.repo", "./worker_repo");
		if (workerRepo != null){
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
		public final static String MODEL_DIR= "/model";
		public final static String RESOURCE_DIR = "/resources";
			
		public final static String ZK_CHROOT = "/feluca";
		public final static String ZK_WORKER_PATH = ZK_CHROOT + "/workers";
		public final static String ZK_LEADER_PATH = ZK_CHROOT + "/leader";
		public final static String FDFS_ZK_ROOT = ZK_CHROOT + "/fdfs";
		
		public final static String FDFS_SERVER_NAME = "FDSFServer";

	}
	
	
	public static class Algorithm{
		public final static String ZK_ALGO_CHROOT = Base.ZK_CHROOT + "/algorithm";
		public final static String ZK_WAITING_PATH = "/workers";
		public final static String ZK_LOOP_PATH = "/loop";
		public final static String ZK_REDUCER_PATH = "/reducer";
		public final static String ZK_MODELSERVER_PATH = "/model";
		
		public final static String LOOPS = "loops";
		public final static String DATANAME = "dataName";
		
		
	}
	
	
	public static class Network{
		public final static String leaderToWorkerText="feluca";		
	}
	
}
