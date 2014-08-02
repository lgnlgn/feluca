package org.shanbo.feluca.distribute.model.vertical;

public interface FloatReducer {
	public float[] reduce(int clientId, float[] orderValues);
	
	public String getName();
}
