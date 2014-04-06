package org.shanbo.feluca.data;

import java.io.IOException;

public class TestDataReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DataReader dataReader = DataReader.createDataReader(false, "data/rcv1");
		dataReader.hasNext();
		long[] offsetArray = dataReader.getOffsetArray();
		int count = 0;
		for(int i = 0 ; i < offsetArray.length; i++){
			Vector vectorByOffset = dataReader.getVectorByOffset(offsetArray[i]);
			if (i < 10)
				System.out.println(vectorByOffset.toString());
			count ++;
		}
		System.out.println(count);

	}

}
