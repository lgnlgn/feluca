package org.shanbo.feluca.distribute.launch;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.distribute.newmodel.VectorClient;
import org.shanbo.feluca.distribute.newmodel.VectorServer;
import org.shanbo.feluca.util.AlgoDeployConf;
import org.shanbo.feluca.util.ZKClient;
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

	//data & computation
	LoopMonitor loopMonitor;
	protected DataReader dataReader; //auto close;
	protected VectorClient vectorClient;
	boolean isDataManager;

	//server & startingGun(with one of the server)
	private VectorServer vectorServer;
	StartingGun startingGun;


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
		loops = conf.getAlgorithmConf().getInteger("loops");
		AlgoDeployConf deployConf = conf.getDeployConf();
		//data server and client can be separated from a worker-node.
		//
		if (deployConf.isDataServer()){
			vectorServer = new VectorServer(conf);
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getModelServers().size());
		}
		if (deployConf.isDataClient()){
			vectorClient = new VectorClient(conf);
		}
		isDataManager = deployConf.isDataManager();
		loopMonitor = new LoopMonitor(conf.getAlgorithmName(), conf.getWorkerName());
	}

	private void openDataInput() throws IOException{
		dataReader = DataReader.createDataReader(false, 
				Constants.Base.getWorkerRepository()+ Constants.Base.DATA_DIR +
				"/" + conf.getDataName());
	}

	protected void createVectorDB() throws Exception{}

	protected void dumpVectorDB() throws Exception{}

	public final void run() throws Exception{
		if (vectorServer!= null){
			vectorServer.start();
		}
		if (vectorClient != null){
			vectorClient.open();
			if (startingGun!= null){
				startingGun.start();
				startingGun.submitAndWait(new Runnable() {
					public void run() {
						try {
							ZKClient.get().setData(Constants.Algorithm.ZK_ALGO_CHROOT + "/" + conf.getAlgorithmName() , new byte[]{});
							createVectorDB();
						} catch (Exception e) {
							throw new FelucaException("createVectorDB error ",e);
						}
					}
				});
			}
			loopMonitor.watchLoopSignal();
			loopMonitor.confirmLoopFinish();
			
			for(int i = 0 ; i < loops && earlyStop()== false;i++){
				System.out.println("loop--:----" + i);
				loopMonitor.waitForLoopStart();
				openDataInput();
				while(dataReader.hasNext()){
					compute();
					dataReader.releaseHolding();
				}
				loopMonitor.confirmLoopFinish();
			}
			if (isDataManager){
				if (startingGun != null){
					startingGun.submitAndWait(new Runnable() {
						public void run() {
							try {
								dumpVectorDB();
							} catch (Exception e) {
								throw new FelucaException("dumpVectorDB error ",e);
							}
						}
					});
					startingGun.setFinish();
				}
			}
			
		}
		loopMonitor.waitForSignalEquals("finish", 10000);
		closeAll();
	}


	/**
	 * todo 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	private void closeAll() throws InterruptedException, KeeperException{
		loopMonitor.close();
		if (vectorClient != null){
			vectorClient.close();
			
		}
		if (vectorServer!= null){
			vectorServer.stop();
		}
		if (isDataManager){
			startingGun.close();
		}
	}

	protected boolean earlyStop(){return false;}

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
	protected abstract void compute() throws Exception;
}
