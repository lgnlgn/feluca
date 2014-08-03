package org.shanbo.feluca.distribute.model.horizon;

public interface MModel{
	
	public final static int PORT_AWAY = 101;
	
	public float[] vectorRetrieve(String vectorName, int[] fids);
	
	public int vectorUpdate(String vectorName, int[] fids, float[] values);
		
	public float[][] matrixRetrieve(String matrixName, int[] fids);
	
	public int matrixUpdate(String matrixName, int[] fids, float[][] values);
	
}