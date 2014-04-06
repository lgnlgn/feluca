package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

public class TestDataReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DataReader dataReader = DataReader.createDataReader(false, "data/mush");
		dataReader.hasNext();
		long[] offsetArray = dataReader.getOffsetArray();
		int count = 0;
		for(int i = 0 ; i < offsetArray.length; i++){
			Vector vectorByOffset = dataReader.getVectorByOffset(offsetArray[i]);
			System.out.println(vectorByOffset.toString());
			count ++;
		}
		System.out.println(count);

	}

}
