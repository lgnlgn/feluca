package org.shanbo.feluca.distribute.model;

import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.net.UnknownHostException;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.DataReader;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.util.NetworkUtils;

import com.alibaba.fastjson.JSONObject;


public class AlgorithmBase{
	
	ModelClient modelClient;
	ModelServer modelServer;
	
	DataReader dataInput;
	GlobalConfig conf;
	JSONObject algoConf;
	public AlgorithmBase(GlobalConfig conf) throws UnknownHostException{
		this.conf = conf;
		this.algoConf = conf.getConfigByNodeAddress(NetworkUtils.getIPv4Localhost().toString());
	}
	
	
	public void init() throws IOException{
		dataInput = DataReader.createDataReader(false,Constants.Base.DATA_DIR + "/" +this.algoConf.getString(Constants.Algorithm.DATANAME).replace("/+", "/"));
		modelClient = new ModelClient(conf);
		modelServer = new ModelServer();
	}
	
	public void close(){
		
	}
	
	public static void distinct(TIntHashSet idSet, Vector v){
		for(int i = 0; i < v.getSize(); i ++){
			idSet.add(v.getFId(i));
		}
	}
	
	
	public void runAlgorithm(){
		Integer loops = algoConf.getInteger(Constants.Algorithm.LOOPS);
		if (loops == null)
			loops = 10;
		for(int i = 0 ; i < loops;i++){
			int batchCurrent = 0;
			TIntHashSet idSet = new TIntHashSet();
			while(dataInput.hasNext()){
				
				long[] offsetArray = dataInput.getOffsetArray();
				 
				for(int o = 0 ; o < offsetArray.length; o++){
					Vector v = dataInput.getVectorByOffset(offsetArray[o]);
					distinct(idSet, v);
				}
			}
		}
	}
	
}
