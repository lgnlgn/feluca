package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;

import java.util.Arrays;
import java.util.HashMap;



public class VectorDBImpl implements VectorDB{

	HashMap<String, float[]> collection ;

	public VectorDBImpl(){
		collection = new HashMap<String, float[]>(3); 
	}

	public synchronized void createVector(String collName, int vectorSize, float defaultValue,boolean overwrite) {
		if (overwrite || !collection.containsKey(collName)){
			float[] values = new float[vectorSize];
			Arrays.fill(values, defaultValue);
			collection.put(collName, values);
		}
	}

	public float[] multiGet(String collName, int[] ids) {
		float[] vector = collection.get(collName);
		if (vector == null){
			return null;
		}else{
			float[] result = new float[ids.length];
			for(int i = 0 ; i < ids.length; i++){
				result[i] = vector[ids[i]];
			}
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

	public void dumpToDisk(String collName, String path) {
		TFloatArrayList list = new TFloatArrayList(collection.get(collName));
		System.out.println(list.toString());
	}

}
