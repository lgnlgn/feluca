package org.shanbo.feluca.data2;

import java.io.IOException;

import org.shanbo.feluca.data2.convert.VectorPartitioner;

public class TestVectorPartitioner {
	public static void main(String[] args) throws IOException {
		VectorPartitioner vp = new VectorPartitioner();
		vp.doPartition("data/real-sim", 1);
	}
}
