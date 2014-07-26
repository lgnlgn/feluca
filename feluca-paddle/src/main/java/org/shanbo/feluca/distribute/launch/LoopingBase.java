package org.shanbo.feluca.distribute.launch;

import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.distribute.model.ModelClient;
import org.shanbo.feluca.distribute.model.ModelServer;

import org.shanbo.feluca.paddle.AlgoDeployConf;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO more dataType support !
 * 
 * 	<p> A batch of vectors are loaded into memory,
 * <p><b> !Remember! Vector is just a ref of byte[], DO NOT store each one by yourself; </b></p>
 *  <p><b> to shuffle or partition this batch, just copy the [offsetArray] and do that to it;</b></p>
 *  <p>basic usage in  {@link #compute()}:
 * <p> {
 *  <p>      long[] offsetArray = dataReader.getOffsetArray();
 *  <p>      
 *  <p>      for(int o = 0 ; o < offsetArray.length; o++){
 *  <p>	        Vector v = dataReader.getVectorByOffset(offsetArray[o]);
 * @author lgn
 *
 */
public abstract class LoopingBase{

	Logger log ;

	protected GlobalConfig conf;
	protected int loops;
	protected int looping;
	
	//data & computation
	LoopMonitor loopMonitor; //with all worker, no matter model or data
	protected DataReader dataReader; //auto close;
	protected ModelClient modelClient;
	boolean isModelManager;

	//server & startingGun(with one of the server)
	private ModelServer vectorServer;
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
		loops = conf.getAlgorithmConf().getInteger(Constants.Algorithm.LOOPS);
		AlgoDeployConf deployConf = conf.getDeployConf();
		//data server and client can be separated from a worker-node.
		//
		if (deployConf.isModelServer()){
			vectorServer = new ModelServer(conf);
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getModelServers().size(), conf.getWorkers().size());
		}
		if (deployConf.isModelClient()){
			modelClient = new ModelClient(conf);
		}
		isModelManager = deployConf.isModelManager();
		loopMonitor = new LoopMonitor(conf.getAlgorithmName(), conf.getWorkerName());
	}

	private void openDataInput() throws IOException{
		dataReader = DataReader.createDataReader(false, 
				Constants.Base.getWorkerRepository()+ Constants.Base.DATA_DIR +
				"/" + conf.getDataName());
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

	/**
	 * call by client that hold's ModelManager role; you may need to create model on remote servers
	 * @throws Exception
	 */
	protected abstract void modelStart() throws Exception;
	/**
	 * call by client that hold's ModelManager role;you may need to dump model on remote servers
	 * @throws Exception
	 */
	protected abstract void modelClose() throws Exception;


	public final void run() throws Exception{
		try{
			if (vectorServer!= null){
				vectorServer.start();
			}
			if (modelClient != null){
				modelClient.open();
				startup();
				if (startingGun!= null){//only one will be started
					startingGun.waitForModelServersStarted(); //wait for model servers started
					System.out.println("StartingGun saw ModelServersStarted");
					ConcurrentExecutor.submitAndWait(new Runnable() { //wait for startup() finished
						public void run() {
							try {
								modelStart();
							} catch (Exception e) {
								throw new FelucaException("createVectorDB error ",e);
							}
						}
					}, 10000);
					System.out.println("modelStarted");
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
				if (isModelManager){ //the only one
					if (startingGun != null){  //do cleanup() first 
						ConcurrentExecutor.submitAndWait(new Runnable() {
							public void run() {
								try {
									modelClose();
								} catch (Exception e) {
									throw new FelucaException("dumpVectorDB error ",e);
								}
							}
						}, 10000);
						startingGun.setFinish(); //tell all workers to finish job
					}
				}
				cleanup();
			}
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
		if (vectorServer!= null){
			vectorServer.stop();
		}
		if (isModelManager){
			startingGun.close();
		}
	}

	/**
	 * early jump from the loops
	 * @return
	 */
	protected boolean earlyStop(){return false;}

	
	
	/**
	 * <p>do computation while needs controlling iteration of data 
	 * <p> if you override this method ; remember add <b>dataReader.releaseHolding();</b> at the end of <b>while(dataReader.hasNext())</b> loop
	 * @throws Exception
	 */
	protected void computeLoop()throws Exception{
		computeLoopBegin();
		while(dataReader.hasNext()){
			computeBlock();
			dataReader.releaseHolding();
		}
		computeLoopEnd();
	}
	
	protected void computeLoopBegin() throws Exception{;}
	protected void computeLoopEnd() throws Exception{;}
		
	
	
	/**
	 * A batch of vectors are loaded into memory,
	 * <p><b> !Remember! Vector is just a ref of byte[], DO NOT store each one by yourself; </b></p>
	 *  <p><b> to shuffle or partition this batch, just copy the [offsetArray] and do that to it;</b></p>
	 *  <p>basic usage here:
	 * <p> {
	 *  <p>      long[] offsetArray = dataReader.getOffsetArray();
	 *  <p>      
	 *  <p>      for(int o = 0 ; o < offsetArray.length; o++){
	 *  <p>	        Vector v = dataReader.getVectorByOffset(offsetArray[o]);
	 *  <p>}
	 */
	protected abstract void computeBlock() throws Exception;
}
