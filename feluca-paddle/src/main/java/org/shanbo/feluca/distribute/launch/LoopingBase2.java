package org.shanbo.feluca.distribute.launch;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.distribute.newmodel.FloatReducerImpl;
import org.shanbo.feluca.distribute.newmodel.ReduceClient;
import org.shanbo.feluca.distribute.newmodel.ReduceServer;
import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * just a test
 * @author lgn
 *
 */
public abstract class LoopingBase2 {
	Logger log ;

	protected GlobalConfig conf;
	protected int loops;
	protected int looping;
	
	//data & computation
	LoopMonitor loopMonitor; //with all worker, no matter model or data
	protected DataEntry dataEntry; //auto close;
	
	boolean isReducer;
	int shardId; 
	protected FloatReducerImpl  reducer; //
	protected ReduceServer reduceServer;
	protected ReduceClient reduceClient;
	
	//server & startingGun(with one of the server)
	StartingGun startingGun; //one and only one with a job
	


	
	public LoopingBase2(GlobalConfig conf) throws Exception{
		log = LoggerFactory.getLogger(this.getClass());
		init(conf);
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void init(GlobalConfig conf) throws Exception{
		this.conf = conf;
		loops = conf.getAlgorithmConf().getInteger(Constants.Algorithm.LOOPS);
		AlgoDeployConf deployConf = conf.getDeployConf();
		//
		if (deployConf.isModelServer()){
			reducer =  getReducer();
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getModelServers().size(), conf.getWorkers().size());
		}
		reduceClient = new ReduceClient(conf);
		loopMonitor = new LoopMonitor(conf.getAlgorithmName(), conf.getWorkerName());
		dataEntry = new DataEntry(Constants.Base.getWorkerRepository() + Constants.Base.DATA_DIR +
				"/" + conf.getDataName());
	}

	private void openDataInput() throws IOException{
		dataEntry.reOpen();
	}
	
	
	
	protected FloatReducerImpl getReducer(){
		return new FloatReducerImpl(conf);
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
	
	public void run() throws Exception{
		try{
			if (reducer!= null){
				reduceServer = new ReduceServer(reducer, conf.getAlgorithmName(),
					Integer.parseInt(conf.getWorkerName().split(":")[1] ) + 100);
				reduceServer.start();
			}
			startup();
			reduceClient.open();
			if (startingGun!= null){//only one will be started
				startingGun.waitForModelServersStarted(); //wait for all reducer servers started
				System.out.println("StartingGun saw ModelServersStarted");
				startingGun.start();//start watch, workers can register it's confirmation
				System.out.println("startingGun.started");
			}
			
			loopMonitor.start(); //wait until start signal & start loop watching 
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
			if (isReducer){ //the only one
				if (startingGun != null){  //do cleanup() first 
					startingGun.setFinish(); //tell all workers to finish job
				}
			}
			cleanup();
			loopMonitor.waitForFinish();
		}finally{
			closeAll();
		}
	}
	private void closeAll() throws InterruptedException, KeeperException{
		loopMonitor.close();
		if (reduceClient != null){
			reduceClient.close();

		}
		if (reduceServer!= null){
			reduceServer.stop();
		}
		if (startingGun !=null){
			startingGun.close();
		}
	}
	protected abstract void computeLoop()throws Exception;

	/**
	 * early jump from the loops
	 * @return
	 */
	protected boolean earlyStop(){return false;}

		
}
