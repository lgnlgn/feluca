package org.shanbo.feluca.distribute.model.horizon;

public interface SyncModel {
	public int vectorUpdate(String vectorName, int[] fids, float[] values);
	
	public int matrixUpdate(String matrixName, int[] fids, float[][] values);
}
