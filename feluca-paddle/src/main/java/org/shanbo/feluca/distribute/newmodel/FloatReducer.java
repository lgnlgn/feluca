package org.shanbo.feluca.distribute.newmodel;

public interface FloatReducer {
	public float[] reduce(int clientId, float[] orderValues);
	
	public String getName();
}
