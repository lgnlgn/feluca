package org.shanbo.feluca.distribute.launch;

import java.io.IOException;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.DataReader;

@Deprecated
public abstract class DataIterator {
	LoopMonitor monitor;
	protected DataReader dataReader;
	protected int loops;
	protected String dataName;
	public DataIterator(String dataName, LoopMonitor monitor, int loops){
		this.dataName=dataName;
		this.monitor = monitor;
	}

	

	public void run() throws Exception{
		monitor.confirmLoopFinish();
		for(int i = 0 ; i < loops && earlyStop()== false;i++){
			monitor.waitForLoopStart();
			initDataInput();
			while(dataReader.hasNext()){
				compute();
				dataReader.releaseHolding();
			}
			monitor.confirmLoopFinish();
		}
	}

	private void initDataInput() throws IOException{
		dataReader = DataReader.createDataReader(false, 
				Constants.Base.getWorkerRepository()+ Constants.Base.DATA_DIR +
				"/" + dataName);
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
