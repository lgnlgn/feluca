package org.shanbo.feluca.distribute.model;

public interface MatrixModel{
	public int vectorCreate(String vectorName, int vectorSize, float defaultValue, boolean overwrite);
	
	public float[] vectorRetrieve(String vectorName, int[] indexIds);
	
	public int vectorUpdate(String vectorName, int[] indexIds, float[] values);
	
	public int vectorDelete(String vectorName);
	
	public int vectorDump(String vectorName, String path, int maxShard, int shardId);
	
	public int matrixCreate(String matrixName, int rowSize, int columnSize, float defaultValue, boolean overwrite);

	public float[][] matrixRetrieve(String matrixName, int[] indexIds);
	
	public int matrixUpdate(String matrixName, int[] indexIds, float[][] values);
	
	public int matrixDelete(String matrixName);

	public int matrixDump(String matrixName, String path, int maxShard, int shardId);
	
	
}