package org.shanbo.feluca.distribute.algorithm.lr;

import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.distribute.model.GlobalConfig;
import org.shanbo.feluca.distribute.model.ModelClient;
import org.shanbo.feluca.distribute.model.ModelServer;
import org.shanbo.feluca.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LR
 * @author lgn
 *
 */
public abstract class AlgorithmBase{

	static Logger log = LoggerFactory.getLogger(AlgorithmBase.class);

	protected ModelClient modelClient;
	protected ModelServer modelServer;

	DataReader dataInput;
	protected GlobalConfig conf;

	public static class IndexOffset{
		int start;
		int end;
		public IndexOffset(int start, int end){
			this.start = start;
			this.end = end;
		}
		public String toString(){
			return "[" + start + ",\t" + end + ")";
		}
	}

	public AlgorithmBase(GlobalConfig conf) throws UnknownHostException{
		this.conf = conf;
	}



	public void init() throws IOException{
		modelClient = new ModelClient(conf);
		int modelSegmentID = conf.modelIndexOf(NetworkUtils.ipv4Host());
		if (modelSegmentID > -1){
			modelServer = new ModelServer(conf, modelSegmentID);
			modelServer.start();
		}
	}
	
	private void initDataInput() throws IOException{
<<<<<<< HEAD:feluca-sail/src/main/java/org/shanbo/feluca/distribute/algorithm/lr/AlgorithmBase.java
		dataInput = DataReader.createDataReader(false, Constants.Base.getWorkerRepository()+ Constants.Base.DATA_DIR +
				"/" + this.conf.getDataName().replace("/+", "/"));
=======
		dataInput = DataReader.createDataReader(false, Constants.Base.getWorkerRepository()+ "/" + this.conf.getString(Constants.Algorithm.DATANAME).replace("/+", "/"));
>>>>>>> c3124c9d14dbbc5e234aed141598dc33da30e24a:feluca-sail/src/main/java/org/shanbo/feluca/distribute/model/AlgorithmBase.java
	}

	

	public void close() throws IOException{
		modelClient.close();
		if (modelServer != null)
			modelServer.stop();
	}

	public static void distinct(TIntHashSet idSet, Vector v){
		for(int i = 0; i < v.getSize(); i ++){
			idSet.add(v.getFId(i));
		}
	}

	static List<IndexOffset> partition(int arrayLength, int partitions){
		int per = arrayLength / partitions;
		if (per < 100){
			per  = 100;
			partitions = arrayLength/ per ;
		}
		List<IndexOffset> result = new ArrayList<AlgorithmBase.IndexOffset>();
		int i = 0 ;
		for(; i < partitions -1 ; i++){
			result.add(new IndexOffset(per*i, per * (i+1)));
		}
		result.add(new IndexOffset(i * per, arrayLength));
		return result;
	}



	abstract protected void compute(Vector v);

	abstract protected void checkStopCondition();

	public void runAlgorithm() throws IOException{
		Integer loops = conf.getAlgorithmConf().getInteger(Constants.Algorithm.LOOPS);
		if (loops == null)
			loops = 10;
		TIntHashSet idSet = new TIntHashSet();
		for(int i = 0 ; i < loops;i++){
			this.initDataInput();
			while(dataInput.hasNext()){
				long[] offsetArray = dataInput.getOffsetArray();
				List<IndexOffset> offsets = partition(offsetArray.length, 10);

				for(IndexOffset indexOffset : offsets){ 
					//batch processing
					for(int o = indexOffset.start ; o < indexOffset.end; o++){
						Vector v = dataInput.getVectorByOffset(offsetArray[o]);
						distinct(idSet, v);
					}
					int[] ids = idSet.toArray();
					try {
						modelClient.fetchModel(ids);
					} catch (Exception e) {
						e.printStackTrace();
						log.error("", e);
					} 
					for(int o = indexOffset.start ; o < indexOffset.end; o++){
						Vector v = dataInput.getVectorByOffset(offsetArray[o]);
						compute(v);//do computation
					}
					try {
						modelClient.updateModel(ids);
					} catch (Exception e) {
						log.error("", e);
					}
					System.out.print("!");
				}
				dataInput.releaseHolding();//release holding
			}
			
			System.out.println("loops:" + i);
			checkStopCondition(); 
			if (modelClient.reachStopCondition()){
				return;
			}
		}
		modelServer.saveModel();
	}

}
