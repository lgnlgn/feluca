package org.shanbo.feluca.distribute.model;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixModelImpl implements MatrixModel{

	static Logger log = LoggerFactory.getLogger(MatrixModelImpl.class);
	HashMap<String, float[]> vectors ; //vector size is determine by creator
	HashMap<String, float[][]> matrixes;
	
	public MatrixModelImpl(){
		vectors = new HashMap<String, float[]>(3);
		matrixes = new HashMap<String, float[][]>(3);
	}
	
	public int vectorCreate(String vectorName, int vectorSize,	float defaultValue, boolean overwrite) {
		log.info("createVector : [" + vectorName + "]  " + vectorSize + "  " + defaultValue);
		if (overwrite || !vectors.containsKey(vectorName)){
			float[] values = new float[vectorSize];
			Arrays.fill(values, defaultValue);
			vectors.put(vectorName, values);
			return 1;
		}else{
			return 0;
		}
	}

	public float[] vectorRetrieve(String vectorName, int[] indexIds) {
		float[] vector = vectors.get(vectorName);
		if (vector == null){
			return null;
		}else{
			float[] result = new float[indexIds.length];
			for(int i = 0 ; i < indexIds.length; i++){
				result[i] = vector[indexIds[i]];
			}
			return result;
		}
	}

	public int vectorUpdate(String vectorName, int[] indexIds, float[] deltaValues) {
		float[] vector = vectors.get(vectorName);
		if (vector != null){
			for(int i = 0; i < indexIds.length; i ++){
				vector[indexIds[i]] = deltaValues[i];
			}
			return 1;
		}
		return 0;
	}

	public int vectorDelete(String vectorName) {
		if (vectors.containsKey(vectorName)){
			vectors.remove(vectorName);
			return 1;
		}
		return 0;
	}

	public int vectorDump(String vectorName, String path, int maxShards,
			int shardId) {
		File out = new File(path + ".vector." + vectorName + "." + shardId);
		out.getParentFile().mkdirs();
		HashPartitioner partitioner = new HashPartitioner(maxShards);
		try{
			float[] values = vectors.get(vectorName);
			BufferedWriter writer = new BufferedWriter(new FileWriter(out));
			for(int i = 0 ; i < values.length; i++){
				writer.write(String.format("%d\t%.6f\n", partitioner.indexToFeatureId(i, shardId), values[i]));
			}
			writer.close();
			return 1;
		}catch (Exception e) {
			log.error("dump vector error !" , e);
			return -1;
		}
	}

	public int matrixCreate(String matrixName, int rowSize, int columnSize,
			float defaultValue, boolean overwrite) {
		log.info("creatematrix : [" + matrixName + "]  (" + rowSize + " , " + columnSize + "): " + defaultValue);
		if (overwrite || !matrixes.containsKey(matrixName)){
			float[][] values = new float[rowSize][];
			for(int i = 0 ; i < rowSize; i++){
				float[] column = new float[columnSize];
				Arrays.fill(column, defaultValue);
				values[i] = column;
			}
			matrixes.put(matrixName, values);
			return 1;
		}else{
			return 0;
		}
	}

	public float[][] matrixRetrieve(String matrixName, int[] indexIds) {
		float[][] matrix = matrixes.get(matrixName);
		if (matrix == null){
			return null;
		}else{
			float[][] result = new float[indexIds.length][];
			for(int i = 0 ; i < indexIds.length; i++){
				result[i] = matrix[indexIds[i]];
			}
			return result;
		}
	}

	public int matrixUpdate(String matrixName, int[] indexIds,float[][] values) {
		float[][] matrix = matrixes.get(matrixName);
		if (matrix != null){
			for(int i = 0; i < indexIds.length; i ++){
				float[] column = matrix[indexIds[i]];
				float[] newValue = values[i];
				for(int j = 0 ; j < column.length; j++)
					column[j] = newValue[j];
			}
			return 1;
		}
		return 0;
	}

	public int matrixDelete(String matrixName) {
		if (matrixes.containsKey(matrixName)){
			matrixes.remove(matrixName);
			return 1;
		}
		return 0;
	}

	public int matrixDump(String matrixName, String path, int maxShards,
			int shardId) {
		File out = new File(path + ".vector." + matrixName + "." + shardId);
		out.getParentFile().mkdirs();
		HashPartitioner partitioner = new HashPartitioner(maxShards);
		try{
			float[][] values = matrixes.get(matrixName);
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(out)));
			for(int i = 0 ; i < values.length; i++){
				oos.write(partitioner.indexToFeatureId(i, shardId));
				oos.writeObject(values[i]);
			}
			oos.flush();
			oos.close();
			return 1;
		}catch (Exception e) {
			log.error("dump vector error !" , e);
			return -1;
		}
	}

}
