package org.shanbo.feluca.distribute.model.horizon;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.HashMap;

import org.shanbo.feluca.util.Utils;

public class ModelCore {
	
	public static class VectorModel{
		public float[] weights;
		TIntArrayList idBuffer;
		TFloatArrayList weightBuffer;
		int bufferSize;
		
		public VectorModel(int weightsLength, int bufferSize, float defaultValue, float vibration){
			this.weights = new float[weightsLength];
			for(int i = 0 ; i < weights.length; i++){
				weights[i] = defaultValue +  (float)(vibration * Utils.randomDouble());
			}
			this.bufferSize = bufferSize;
			this.idBuffer = new TIntArrayList(512);
			this.weightBuffer = new TFloatArrayList(512);
		}
		
		public void clear(){
			if (idBuffer.size() > Math.max(512,bufferSize)){
				this.idBuffer = new TIntArrayList(512);
				this.weightBuffer = new TFloatArrayList(512);
			}else{
				this.idBuffer.resetQuick();
				this.weightBuffer.resetQuick();
			}
		}
		
		public void fidToBufer(int fid){
			this.idBuffer.add(fid);
			this.weightBuffer.add(weights[fid]);
		}
		
		public int[] getIds(){
			return idBuffer.toArray();
		}
		
		public float[] getWeights(){
			return weightBuffer.toArray();
		}
	}
	
	public static class MatrixModel{
		public float[][] matrix;
		TIntArrayList idBuffer;
		ArrayList<float[]> weightBuffer; //ref
		int bufferSize;
		
		public MatrixModel(int rowSize, int columnSize, int bufferSize, float defaultValue, float vibration){
			this.matrix = new float[rowSize][];
			for(int i = 0 ; i < matrix.length; i++){
				matrix[i] = new float[columnSize];
				for(int j = 0 ; j < columnSize; j++){
					matrix[i][j] = defaultValue + (float)(vibration * Utils.randomDouble());
				}
			}
			this.bufferSize = bufferSize;
			this.idBuffer = new TIntArrayList(512);
			this.weightBuffer = new ArrayList<float[]>(512);
		}
		
		public void clear(){
			if (idBuffer.size() > Math.max(512,bufferSize)){
				this.idBuffer = new TIntArrayList(512);
				this.weightBuffer = new ArrayList<float[]>(512);
			}else{
				this.idBuffer.resetQuick();
				this.weightBuffer.clear();
			}
		}
		
		public void fidToBufer(int fid){
			this.idBuffer.add(fid);
			this.weightBuffer.add(matrix[fid]);
		}
		
		public int[] getIds(){
			return idBuffer.toArray();
		}
		
		public float[][] getWeights(){
			float[][] result = new float[weightBuffer.size()][];
			for(int i = 0; i < weightBuffer.size(); i++){
				result[i] = weightBuffer.get(i);
			}
			return result;
		}
	}

	HashMap<String, VectorModel> vectors ; //vector size is determine by creator
	HashMap<String, MatrixModel> matrixes;

	public ModelCore(){
		vectors = new HashMap<String, ModelCore.VectorModel>(3);
		matrixes = new HashMap<String, ModelCore.MatrixModel>(3);
	}
	
	
	public VectorModel getVector(String vectorName){
		return vectors.get(vectorName);
	}
	
	public MatrixModel getMatrix(String matrixName){
		return matrixes.get(matrixName);
	}

	
	public void createVector(String vectorName, int weightsLength, int bufferSize, float defaultValue, float vibration){
		vectors.put(vectorName, new VectorModel(weightsLength, bufferSize, defaultValue, vibration));
	}
	
	public void createMatrix(String matrixName, int rowSize, int columnSize, int bufferSize, float defaultValue, float vibration){
		matrixes.put(matrixName, new MatrixModel(rowSize, columnSize, bufferSize, defaultValue, vibration));
	}
}
