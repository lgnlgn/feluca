package org.shanbo.feluca.node.leader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;









import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.datasys.DataClient;
import org.shanbo.feluca.node.NodeRole;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class LeaderModule extends RoleModule{
	
	private JobManager jobManager;
	private volatile Map<String, String> workers;
	private String dataDir;
	public ClientBootstrap bootstrap;
	public HttpClient actionClient;
	
	
	private ChildrenWatcher cw;

	public LeaderModule() throws KeeperException, InterruptedException{
		this.role = NodeRole.Leader;
		workers = new ConcurrentHashMap<String, String>();
		dataDir = Constants.DATA_DIR;
		bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
		
	    ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager();
	    actionClient = new DefaultHttpClient(mgr);
		ZKClient.get().createIfNotExist(Constants.ZK_LEADER_PATH);
		ZKClient.get().createIfNotExist(Constants.ZK_WORKER_PATH);
		this.watchZookeeper();
		this.jobManager = new JobManager();
	}

	
	private void addSlave(String hostPort){
		this.workers.put(hostPort, "");
	}
	
	private String removeSlave(String hostPort){
		return this.workers.remove(hostPort);
	}
	
	public String getDataDir(){
		return this.dataDir;
	}
	
	public Map<String, String> copySlave(){
		Map<String, String> slaves = new ConcurrentHashMap<String, String>();
		for(String slaveAddress : this.workers.keySet()){
			slaves.put(slaveAddress, null);
		}
		return slaves;
	}
	
	public boolean dataSetExist(String dataName){
		return new File(dataDir + "/" + dataName).isDirectory();
	}
	
	
	public String submitJob(Class<? extends FelucaJob> clz, Properties conf) throws Exception{
		if (clz == null)
			return null;
		return this.jobManager.asynRunJob(clz, conf);
	}
	
	public String killJob(String jobName){
		if (StringUtils.isBlank(jobName))
			return "jobName empty!?";
		return 
			this.jobManager.killJob(jobName);
	}
	
	
	public String getJobStatus(){
		return jobManager.getCurrentJobState();
	}
	
	
	public JSONArray getLatestJobStatus(int last){
		return jobManager.getLatestJobStates(last);
	}
	

	public void watchZookeeper(){
		cw = new ChildrenWatcher() {
			@Override
			public void nodeRemoved(String node) {
				removeSlave(node);
			}
			@Override
			public void nodeAdded(String node) {
				addSlave(node);
			}
		};
		ZKClient.get().watchChildren(Constants.ZK_WORKER_PATH, cw);
	}
	
	
	public List<String> yieldSlaves(){
		List<String> a = new ArrayList<String>();
		a.addAll(this.workers.keySet());
		return a;
	}
	
//	public String requestSlaves(AbstractDistributedChannelHandler tsReq) throws InterruptedException, ExecutionException{
//		return this.requestSlaves(tsReq, 2000);
//		List<String> slaves = yieldSlaves();
//		List<Callable<String >> distributedReqs = new ArrayList<Callable<String >> ();
//		for(String slaveAddr: slaves){
//			distributedReqs.add(new HttpRequestCallable(httpClient, "http://" + slaveAddr + tsReq.requestSlaveUri()));
//		}
//		List<String> results = ConcurrentExecutor.execute(distributedReqs);
//		return results.toString();
//	}
	
	/**
	 * TODO
	 * @return
	 */
	public JSONObject localDataSet(){
		return null;
	}
	
	/**
	 * TODO
	 * @return
	 */
	public JSONObject localDataSetInfo(String dataName){
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public JSONObject gatherRemoteDataSet(){
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	public JSONObject gatherRemoteDataSetInfo(String dataName){
		return null;
	}
	
	
	
	public ClientBootstrap getBootstrap(){
		return this.bootstrap;
	}
	

}
