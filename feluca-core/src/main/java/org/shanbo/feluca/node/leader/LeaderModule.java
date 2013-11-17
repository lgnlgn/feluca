package org.shanbo.feluca.node.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;





import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaJob;
import org.shanbo.feluca.node.RoleModule;
import org.shanbo.feluca.util.ZKClient;
import org.shanbo.feluca.util.ZKClient.ChildrenWatcher;

import com.alibaba.fastjson.JSONArray;

public class LeaderModule extends RoleModule{
	
	private JobManager jobManager;
	private volatile Map<String, String> workers;
	private String dataDir;
	public ClientBootstrap bootstrap;
	public HttpClient httpClient;

	private ChildrenWatcher cw;

	public LeaderModule() throws KeeperException, InterruptedException{
		workers = new ConcurrentHashMap<String, String>();
		dataDir = Constants.DATA_DIR;
		bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
		
	    ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager();
	    httpClient = new DefaultHttpClient(mgr);
		ZKClient.get().createIfNotExist(Constants.ZK_LEADER_PATH);
		ZKClient.get().createIfNotExist(Constants.ZK_WORKER_PATH);
		this.watchZookeeper();
		this.jobManager = new JobManager();
	}

	
	public void addSlave(String hostPort){
		this.workers.put(hostPort, "");
	}
	
	public String removeSlave(String hostPort){
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
	
	public boolean submitJob(Class<? extends FelucaJob> clz, Properties conf) throws Exception{
		if (clz == null)
			return false;
		return this.jobManager.asynRunJob(clz, conf);
	}
	
	public String getJobStatus(){
		return jobManager.getCurrentJobState();
	}
	
	public String getLatestJobStatus(int num){
		JSONArray ja = jobManager.getLatestJobStates();
		JSONArray jaa = new JSONArray();
		int n = 0;
		for(int i = ja.size()-1; i >= 0 && n < num; n++,i--){
			jaa.add(ja.get(i));
		}
		return jaa.toString();
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
	
	public void addMessageToJob(String content){
		this.jobManager.addMessageToJob(content);
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
	

	
	public ClientBootstrap getBootstrap(){
		return this.bootstrap;
	}
	

	
}
