package org.shanbo.feluca.distribute.model.horizon;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;

import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MModelImpl implements MModel{

	static Logger log = LoggerFactory.getLogger(MModelImpl.class);
	HashMap<String, float[]> vectors ; //vector size is determine by creator
	HashMap<String, float[][]> matrixes;
	
	public MModelImpl(){
		vectors = new HashMap<String, float[]>(5);
		matrixes = new HashMap<String, float[][]>(5);
	}
	
	public int vectorCreate(String vectorName, int vectorSize,	float defaultValue, float vibration) {
		log.info("createVector : [" + vectorName + "]  " + vectorSize + "  " + defaultValue);
		if ( !vectors.containsKey(vectorName)){
			float[] values = new float[vectorSize];
			for(int i = 0 ; i < values.length; i++){
				values[i] = defaultValue +  (float)(vibration * Utils.randomDouble());
			}
			vectors.put(vectorName, values);
			return 1;
		}else{
			return 0;
		}
	}

	public float[] vectorRetrieve(String vectorName, int[] fids) {
		float[] vector = vectors.get(vectorName);
		if (vector == null){
			return null;
		}else{
			float[] result = new float[fids.length];
			for(int i = 0 ; i < fids.length; i++){
				result[i] = vector[fids[i]];
			}
			return result;
		}
	}

	public int vectorUpdate(String vectorName, int[] fids, float[] values) {
		float[] vector = vectors.get(vectorName);
		if (vector != null){
			for(int i = 0; i < fids.length; i ++){
				vector[fids[i]] = values[i];
			}
			return 1;
		}
		return 0;
	}



	public int vectorDump(String vectorName, String path, int maxShards, int shardId) {
		File out = new File(path + ".vector." + vectorName + "." + shardId);
		out.getParentFile().mkdirs();
		HashPartitioner partitioner = new HashPartitioner(maxShards);
		try{
			float[] values = vectors.get(vectorName);
			BufferedWriter writer = new BufferedWriter(new FileWriter(out));
			for(int i = 0 ; i < values.length; i++){
				writer.write(String.format("%d\t%.6f\n", partitioner.indexToId(i, shardId), values[i]));
			}
			writer.close();
			return 1;
		}catch (Exception e) {
			log.error("dump vector error !" , e);
			return -1;
		}
	}

	public int matrixCreate(String matrixName, int rowSize, int columnSize,
			float defaultValue, float vibration) {
		log.info("creatematrix : [" + matrixName + "]  (" + rowSize + " , " + columnSize + "): " + defaultValue);
		if ( !matrixes.containsKey(matrixName)){
			float[][] matrix = new float[rowSize][];
			for(int i = 0 ; i < matrix.length; i++){
				matrix[i] = new float[columnSize];
				for(int j = 0 ; j < columnSize; j++){
					matrix[i][j] = defaultValue + (float)(vibration * Utils.randomDouble());
				}
			}
			matrixes.put(matrixName, matrix);
			return 1;
		}else{
			return 0;
		}
	}

	public float[][] matrixRetrieve(String matrixName, int[] fids) {
		float[][] matrix = matrixes.get(matrixName);
		if (matrix == null){
			return null;
		}else{
			float[][] result = new float[fids.length][];
			for(int i = 0 ; i < fids.length; i++){
				result[i] = matrix[fids[i]];
			}
			return result;
		}
	}

	public int matrixUpdate(String matrixName, int[] fids, float[][] values) {
		float[][] matrix = matrixes.get(matrixName);
		if (matrix != null){
			for(int i = 0; i < fids.length; i ++){
				float[] column = matrix[fids[i]];
				float[] newValue = values[i];
				for(int j = 0 ; j < column.length; j++)
					column[j] = newValue[j];
			}
			return 1;
		}
		return 0;
	}



	public int matrixDump(String matrixName, String path, int maxShards,
			int shardId) {
		File out = new File(path + ".matrix." + matrixName + "." + shardId);
		if (out.getParentFile() != null) 
			out.getParentFile().mkdirs();
		HashPartitioner partitioner = new HashPartitioner(maxShards);
		try{
			float[][] values = matrixes.get(matrixName);
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(out)));
			for(int i = 0 ; i < values.length; i++){
				oos.write(partitioner.indexToId(i, shardId));
				oos.writeObject(values[i]);
			}
			debug(values, partitioner, shardId);
			oos.flush();
			oos.close();
			return 1;
		}catch (Exception e) {
			log.error("dump vector error !" , e);
			return -1;
		}
	}

	private void debug(float[][] values, HashPartitioner partitioner, int shardId){
		for(int i = 0 ; i < values.length; i++){
			StringBuilder builder = new StringBuilder();
			builder.append(partitioner.indexToId(i, shardId) + "\t[");
			for(int j = 0 ; j< values[i].length; j++){
				builder.append(values[i][j] + " ,");
			}
 			builder.append("]");
 			System.out.println(builder.toString());
		}
	}


	
}
