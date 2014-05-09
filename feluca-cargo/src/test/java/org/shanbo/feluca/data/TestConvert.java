package org.shanbo.feluca.data;

import java.io.IOException;

import org.shanbo.feluca.data.convert.DataConverter;

public class TestConvert {
	public static void main(String[] args) throws IOException {
		DataConverter dc = new DataConverter("G:/covtype.dat");
//		dc.convertFID2FID("data/mush");
		dc.convertLW2LW("data/covtype");
	}
}
