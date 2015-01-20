package org.shanbo.feluca.data2;

import java.io.IOException;

import org.shanbo.feluca.data2.convert.VectorSerializer;

public class TestVectorSerializer {
	public static void main(String[] args) throws IOException {
		String file = "/home/lgn/kaggle/avazutrain33.txt";
		VectorSerializer vs = new VectorSerializer(file);
//		vs.convertTuple2VID("data/mltest");
		vs.convertLW2LW("/home/lgn/data/avazutrain33");
	}
}
