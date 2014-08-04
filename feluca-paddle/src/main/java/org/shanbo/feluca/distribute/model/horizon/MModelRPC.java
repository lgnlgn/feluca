package org.shanbo.feluca.distribute.model.horizon;

public interface MModelRPC{
	
	public final static int PORT_AWAY = 5;
	
	public float[] vectorRetrieve(String vectorName, int[] fids);
	
	public int vectorUpdate(String vectorName, int[] fids, float[] values);
		
	public float[][] matrixRetrieve(String matrixName, int[] fids);
	
	public int matrixUpdate(String matrixName, int[] fids, float[][] values);
	
}