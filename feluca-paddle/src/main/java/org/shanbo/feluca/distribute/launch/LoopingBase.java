package org.shanbo.feluca.distribute.launch;

import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.distribute.model.horizon.MModelClient;
import org.shanbo.feluca.distribute.model.horizon.MModelLocal;
import org.shanbo.feluca.distribute.model.horizon.MModelServer;
import org.shanbo.feluca.distribute.model.vertical.FloatReducerClient;
import org.shanbo.feluca.distribute.model.vertical.ReduceServer;


import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.paddle.GlobalConfig;
import org.shanbo.feluca.util.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoopingBase implements Runnable{

	Logger log ;

	protected GlobalConfig conf;
	protected int loops;
	protected int looping;
	protected int shardId;
	protected boolean useSyncModel;
	//data & computation
	LoopMonitor loopMonitor; //with all worker, no matter model or data
	protected DataEntry dataEntry; //auto close;

	protected ReduceServer reduceServer;
	protected FloatReducerClient reducerClient;

	//server & startingGun(with one of the server)
	protected MModelLocal local;
	protected MModelClient modelClient;
	protected MModelServer modelServer;
	StartingGun startingGun; //one and only one with a job

	public static void distinctIds(TIntHashSet idSet, Vector v){
		for(int i = 0; i < v.getSize(); i ++){
			idSet.add(v.getFId(i));
		}
	}


	public LoopingBase(GlobalConfig conf) throws Exception{
		log = LoggerFactory.getLogger(this.getClass());
		init(conf);
		dataEntry = new DataEntry(Constants.Base.getWorkerRepository() + Constants.Base.DATA_DIR + "/" + conf.getDataName(), 
				"\\.v\\." + this.shardId + "\\.dat");
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void init(GlobalConfig conf) throws Exception{
		this.conf = conf;
		local = new MModelLocal();
		shardId = conf.getShardId();
		loops = conf.getAlgorithmConf().getInteger(Constants.Algorithm.LOOPS);
		useSyncModel = JSONUtil.getConf(conf.getAlgorithmConf(), Constants.Algorithm.OPEN_MODEL_SERVER, false);
		AlgoDeployConf deployConf = conf.getDeployConf();
		//data server and client can be separated from a worker-node.
		//
		if (deployConf.isReduceServer()){
			reduceServer = new ReduceServer(conf.getWorkerName(), conf.getWorkers().size(), conf.getAlgorithmName());
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getReduceServers().size(), conf.getWorkers().size());
		}
		if (useSyncModel){
			modelServer = new MModelServer(conf.getWorkerName(), conf.getAlgorithmName(), local);			
		}
		reducerClient = new FloatReducerClient(conf.getReduceServers(), shardId); 
		modelClient = new MModelClient(conf.getWorkers(), shardId, local);
		loopMonitor = new LoopMonitor(conf.getAlgorithmName(), conf.getWorkerName());
	}

	private void openDataInput() throws IOException{
		dataEntry.reOpen();
	}

	/**
	 * initial your members, you may need to create PartialModel for Client
	 * @throws Exception
	 */
	protected void startup() throws Exception{}

	/**
	 * release your resources
	 * @throws Exception
	 */
	protected void cleanup() throws Exception{}


	public final void run(){
		try{
			if (reduceServer!= null){
				reduceServer.start();
			}
			if (modelServer != null){
				modelServer.start();
			}
			startup();
			if (startingGun!= null){//only one will be started
				startingGun.waitForModelServersStarted(); //wait for all servers started
				System.out.println("StartingGun saw all servers Started");
				startingGun.start();//start watch, workers can register it's confirmation
				System.out.println("startingGun.started");
			}
			loopMonitor.start(); //wait until start signal(reduceServers & modelServers all started) then start loop watching 
			reducerClient.connect();//connecting; algorithms always use reducer instead of syncModel  
			if (useSyncModel){
				modelClient.connect(); //
			}
			
			System.out.println("loopMonitor.started");
			loopMonitor.confirmLoopFinish(); //tell startingGun I'm ok
			for(looping = 0 ; looping < loops && earlyStop() == false;looping++){
				System.out.print("loop--:----(" + looping);
				loopMonitor.waitForLoopStart();   //wait for other workers; according to startingGun's action 
				System.out.println(")");
				openDataInput();
				computeLoop();
				loopMonitor.confirmLoopFinish();
			}
			if (useSyncModel){
				modelClient.close(); //
			}
			reducerClient.close();
			if (startingGun != null){  //do cleanup() first 
				startingGun.setFinish(); //tell all workers to finish job
			}
			cleanup();
			loopMonitor.waitForFinish(); //wait for finish signal
		}catch (Exception e) {
			log.error( " exception during running " ,e);
		}

		finally	{
			try {
				closeAll();
			} catch (Exception e) {
				log.error( " close error " ,e);
			}
		}
	}


	/**
	 * todo 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	private void closeAll() throws InterruptedException, KeeperException{
		loopMonitor.close();

		if (reduceServer!= null){
			reduceServer.stop();
		}
		if (modelServer!= null){
			modelServer.stop();
		}
		if (startingGun!= null){
			startingGun .close();
		}
	}

	/**
	 * early jump from the loops
	 * @return
	 */
	protected boolean earlyStop(){return false;}

	protected abstract void computeLoop()throws Exception;

}
