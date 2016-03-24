package org.shanbo.feluca.model;

public interface FloatReducer {
	public float[][] reduce(String op, float[][] data, int shardId);
}
