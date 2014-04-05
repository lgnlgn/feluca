package org.shanbo.feluca.data;

import java.io.IOException;

public class TestConvert {
	public static void main(String[] args) throws IOException {
		DataConverter dc = new DataConverter("e:/data/mushroom.dat");
		dc.convertFID2FID("data/mush");
	}
}
