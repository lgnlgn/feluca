package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class VectorDBImpl implements VectorDB{

	Logger log = LoggerFactory.getLogger(VectorDBImpl.class);
	HashMap<String, float[]> collection ;
	HashMap<String, Float> defaultValues;
	Map<String, Integer> fidMaxMap;
	
	public VectorDBImpl(){
		collection = new HashMap<String, float[]>(3); 
		defaultValues = new HashMap<String, Float>(3);
		fidMaxMap = new HashMap<String, Integer>(3);
	}

	public synchronized void createVector(String collName, int vectorSize, float defaultValue,boolean overwrite) {
		System.out.println("createVector : [" + collName + "]  " + vectorSize + "  " + defaultValue);
		if (overwrite || !collection.containsKey(collName)){
			float[] values = new float[vectorSize];
			Arrays.fill(values, defaultValue);
			collection.put(collName, values);
			defaultValues.put(collName, defaultValue);
			fidMaxMap.put(collName, 0);
		}
	}

	public float[] multiGet(String collName, int[] ids) {
		float[] vector = collection.get(collName);
		Integer fid = fidMaxMap.get(collName);
		
		if (vector == null){
			return null;
		}else{
			int fidMax = fid.intValue();
			float[] result = new float[ids.length];
			for(int i = 0 ; i < ids.length; i++){
				fidMax = ids[i] > fidMax ? ids[i] : fidMax;
				result[i] = vector[ids[i]];
			}
			fidMaxMap.put(collName, fidMax);
			return result;
			
		}
	}

	public void multiUpdate(String collName, int[] ids, float[] deltaValues) {
		float[] vector = collection.get(collName);
		if (vector != null){
			for(int i = 0; i < ids.length; i ++){
				vector[ids[i]] += deltaValues[i];
			}
		}
	}

	/**
	 * TODO
	 */
	public synchronized void dumpToDisk(String collName, String path, int maxShards, int shardId) {
		//
		TFloatArrayList list = new TFloatArrayList(collection.get(collName));
		System.out.println(list.toString());
		File out = new File(path + "." + collName + "." + shardId);
		out.getParentFile().mkdirs();
		FidPartitioner partitioner = new FidPartitioner.HashPartitioner(maxShards);
		try{
			float[] values = collection.get(collName);
			int fidMax = fidMaxMap.get(collName).intValue();
			float defaultValue = defaultValues.get(collName).floatValue();
			BufferedWriter writer = new BufferedWriter(new FileWriter(out));
			for(int i = 0 ; i <= fidMax; i++){
				if (values[i] != defaultValue)
					writer.write(String.format("%d\t%.6f\n", partitioner.indexToFeatureId(i, shardId), values[i]));
			}
			writer.close();
		}catch (Exception e) {
			log.error("dump vector error !" , e);
		}
	}

}
