package org.shanbo.feluca.data2;

import java.io.IOException;

import org.shanbo.feluca.data2.convert.VectorConverter;

public class TestVectorConverter {

	public static void main(String[] args) throws IOException {
		String file = RawFiles.realsim;
		VectorConverter vs = new VectorConverter(file);
//		vs.convertTuple2VID("data/mltest");
		vs.convertLW2LW("data/realsim");
	}

}
