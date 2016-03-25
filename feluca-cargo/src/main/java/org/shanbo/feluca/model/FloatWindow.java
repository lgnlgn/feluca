package org.shanbo.feluca.model;

import java.util.Arrays;
import java.util.Map;

import gnu.trove.list.array.TFloatArrayList;

/**
 * 
 * @author lgn
 *
 */
  class FloatWindow {

	float initValue = Float.NEGATIVE_INFINITY;
	float[][] list;
	float[] keys;
	
	private int currentIndex = 0;

	public int getCapacity(){
		return list.length;
	}
	
	public FloatWindow(int size, int range){
		keys = new float[size];
		reset();
		list = new float[size][];
//		for(int i = 0 ; i < size ; i++){
//			list[i] = new float[range];
//		}
	}
	
	public void set(int ith, int offset, float value){
		set(ith);
		list[ith][offset] = value;
	}
	
	
	public void set(int ith){
		keys[ith] = ith;
	}
	
	void init(int ith, float[] array){
		list[ith] = array;
	}
	
	public float[] getList(int index){
		return list[index];
	}
	
	public void reset(){
		Arrays.fill(keys, initValue);
	}
	
	public int usedRow(){
		int c = 0;
		for(int i = 0 ; i < keys.length; i++){
			if (keys[i] != initValue){
				c += 1;
			}
		}
		return c;
	}
	
	public String toString(){
		return orignalFormat();
	}
	
	public String zippedFormat(){
		return toString(zip());
	}
	
	public String orignalFormat(){
		StringBuilder a = new StringBuilder(new TFloatArrayList(keys).subList(0, Math.min(keys.length, 15)).toString() + "\n[");
		for(int i = 0 ; i < Math.min(keys.length, 15); i++){
			a.append(new TFloatArrayList(list[i]).toString() + ",");
		}
		a.append("...]");
		return a.toString();
	}
	
	public float[][] zip(){
		
		if (list[0].length < 2){ //always convert to an array
			float[][] result = new float[2][];
			result[0] = new float[]{-1};
			result[1] = new float[keys.length];
			for(int i = 0 ; i < keys.length; i++){
				result[1][i] = list[i][0];
			}
			return result;
		}else{    //maybe an empty matrix
			int usedRows = usedRow();
			float[][] result = new float[usedRows + 1][];
			float[] onlyKeys = new float[usedRows];
			result[0] = onlyKeys;
			for(int i = 0, j = 1 ; i < keys.length; i++){
				if (keys[i] != initValue){
					onlyKeys[j-1] = keys[i];
					result[j] = list[i];
//					result[j] = new float[list[i].length];
//					System.arraycopy(list[i], 0, result[j], 0, list[i].length);
					j ++;
				}
			}
			return result;
		}
	}
	
	static boolean isZipped(float[][] result){
		if (result[0].length == 1 &&  result[0][0] == -1){
			return true;
		}
		return false;
	}
	
	/**
	 * 
	 * @param result
	 */
	public void unzipAndSet(float[][] result){
		if (isZipped(result)){
			for(int i = 0; i < keys.length; i++){
				list[i][0] = result[1][i];
			}
		}else{
			for(int j = 0; j < result[0].length; j++){
				keys[(int)result[0][j]] = result[0][j];
				list[(int)result[0][j]] = result[j+1];
			}
//			for(int i = 0, j = 1 ; i < keys.length; i++){
//				if (keys[i] != initValue){
////					System.arraycopy(result[j], 0, list[(int)keys[i]], 0, list[i].length);
//					list[(int)keys[i]] = result[j];
//					j++;
//				}
//			}
		}
	}
	
	public static String toString(float[][] data){
		StringBuilder a = new StringBuilder();
		for(int i = 0 ; i < Math.min(data.length, 15); i++){
			a.append(new TFloatArrayList(data[i]).toString() + "\n");
		}
		a.append("...]");
		return a.toString();
	}
	
	public static float[][] clone(float[][] data){
		float[][] clone = new float[data.length][];
		for(int i = 0 ; i < data.length; i++){
			clone[i] = new float[data[i].length];
			System.arraycopy(data[i], 0, clone[i], 0, data[i].length);
		}
		return clone;
	}
}
