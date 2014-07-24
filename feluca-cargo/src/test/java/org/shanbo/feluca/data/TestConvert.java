package org.shanbo.feluca.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.shanbo.feluca.data.convert.DataConverter;

public class TestConvert {
	public static void main(String[] args) throws IOException {
		String movielens = "E:/data/ml-10M100K/movielens.txt.test";
		DataConverter dc = new DataConverter(movielens);
//		dc.convertFID2FID("data/mush");
//		dc.convertLW2LW("data/real-sim");
		dc.convertTuple2VID("data/movielens_test", Tuple.AlignColumn.FIRST);
	}
}
