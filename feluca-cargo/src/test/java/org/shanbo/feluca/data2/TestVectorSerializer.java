package org.shanbo.feluca.data2;

import java.io.IOException;

import org.shanbo.feluca.data2.convert.VectorSerializer;

public class TestVectorSerializer {
	public static void main(String[] args) throws IOException {
		VectorSerializer vs = new VectorSerializer(RawFiles.movielensTrain);
		vs.convertTuple2VID("data/mltrain");
	}
}
