package org.shanbo.feluca.distribute.launch;

import java.io.IOException;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.distribute.newmodel.VectorClient;
import org.shanbo.feluca.distribute.newmodel.VectorServer;
import org.shanbo.feluca.util.AlgoDeployConf;

/**
 * TODO more dataType support !
 * @author lgn
 *
 */
public abstract class LoopingBase{
	protected GlobalConfig conf;
	protected int loops;
	protected String dataName;


	//data & computation
	LoopMonitor monitor;
	protected DataReader dataReader; //auto close;
	protected VectorClient vectorClient;
	boolean isDataManager = false;

	//server & startingGun(with one of the server)
	private VectorServer vectorServer;
	StartingGun startingGun;


	public LoopingBase(GlobalConfig conf) throws Exception{
		init();
	}

	private void init() throws Exception{
		AlgoDeployConf deployConf = conf.getDeployConf();
		if (deployConf.isDataServer()){
			vectorServer = new VectorServer(conf, conf.getString("worker"));
			vectorServer.start();
		}
		if (deployConf.isStartingGun()){
			startingGun = new StartingGun(conf.getAlgorithmName(), conf.getModelServers().size());
		}
		if (deployConf.isDataClient()){
			vectorClient = new VectorClient(conf);
		}
		isDataManager = deployConf.isDataManager();
	}

	private void openDataInput() throws IOException{
		dataReader = DataReader.createDataReader(false, 
				Constants.Base.getWorkerRepository()+ Constants.Base.DATA_DIR +
				"/" + dataName);
	}

	public abstract void createVectorDB();

	public abstract void dumpVectorDB();

	public void run() throws Exception{
		if (vectorServer!= null){
			vectorServer.start();
		}
		if (vectorClient != null){
			if (startingGun!= null){
				startingGun.wait(new Runnable() {
					public void run() {
						createVectorDB();
					}
				});
			}
			monitor.confirmLoopFinish();
			for(int i = 0 ; i < loops && earlyStop()== false;i++){
				monitor.waitForLoopStart();
				openDataInput();
				while(dataReader.hasNext()){
					compute();
					dataReader.releaseHolding();
				}
				monitor.confirmLoopFinish();
			}
			if (isDataManager){
				if (startingGun != null){
					startingGun.wait(new Runnable() {
						public void run() {
							dumpVectorDB();
						}
					});
					startingGun.setFinish();
				}
			}
			monitor.waitForSignalEquals("finish", 10000);
		}
		close();
	}


	/**
	 * todo 
	 * @throws InterruptedException 
	 */
	public void close() throws InterruptedException{
		
		if (vectorServer!= null){
			vectorServer.stop();
		}
		if (vectorClient != null){
			vectorClient.close();
		}
	}

	public abstract boolean earlyStop();

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
	protected abstract void compute();
}
