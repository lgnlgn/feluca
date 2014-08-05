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

public abstract class LoopingBase{

	Logger log ;

	protected GlobalConfig conf;
	protected int loops;
	protected int looping;

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
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void init(GlobalConfig conf) throws Exception{
		this.conf = conf;
		local = new MModelLocal();
		loops = conf.getAlgorithmConf().getInteger(Constants.Algorithm.LOOPS);
		AlgoDeployConf deployConf = conf.getDeployConf();
		//data server and client can be separated from a worker-node.
		//
		if (deployConf.isReduceServer()){
			reduceServer = new ReduceServer(conf.getWorkerName(), conf.getWorkers().size(), conf.getAlgorithmName());
			
		}
		reducerClient = new FloatReducerClient(conf.getReduceServers(), conf.getShardId()); 
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getReduceServers().size(), conf.getWorkers().size());
		}

		if (JSONUtil.getConf(conf.getAlgorithmConf(), Constants.Algorithm.OPEN_MODEL_SERVER, false)){
			modelServer = new MModelServer(conf.getWorkerName(), conf.getAlgorithmName(), local);
			modelClient = new MModelClient(conf.getWorkers(), conf.getShardId(), local);
		}
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


	public final void run() throws Exception{
		try{
			if (reduceServer!= null){
				reduceServer.start();
			}
			if (modelServer != null){
				modelServer.start();
			}
			startup();
			reducerClient.connect();
			if (startingGun!= null){//only one will be started
				startingGun.waitForModelServersStarted(); //wait for all servers started
				System.out.println("StartingGun saw all servers Started");
				startingGun.start();//start watch, workers can register it's confirmation
				System.out.println("startingGun.started");
			}
			
			loopMonitor.start(); //wait until start signal & start loop watching 
			if (modelClient != null){
				modelClient.connect();
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

			if (startingGun != null){  //do cleanup() first 
				startingGun.setFinish(); //tell all workers to finish job
			}
			cleanup();
			loopMonitor.waitForFinish(); //wait for finish signal
		}catch (Exception e) {
			e.printStackTrace();
		}

		finally	{
			closeAll();
		}
	}


	/**
	 * todo 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	private void closeAll() throws InterruptedException, KeeperException{
		loopMonitor.close();
		if (modelClient != null){
			modelClient.close();
		}
		reducerClient.close();
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
