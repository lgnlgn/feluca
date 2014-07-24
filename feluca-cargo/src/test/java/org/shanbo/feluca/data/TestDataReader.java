package org.shanbo.feluca.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TestDataReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DataReader dataReader = DataReader.createDataReader(false, "data/movielens_train");
		int num = 0;
		BufferedWriter writer = new BufferedWriter(new FileWriter("data/movielens.train.txt"));
		while(dataReader.hasNext()){
			long[] offsetArray = dataReader.getOffsetArray();
			int count = 0;
			for(int i = 0 ; i < offsetArray.length; i++){
				Vector vectorByOffset = dataReader.getVectorByOffset(offsetArray[i]);
//				writer.write(vectorByOffset.toString() + "\n");
//				if (i < 10)
//					System.out.println(vectorByOffset.toString());
				count ++;

			}
			System.out.println(count);
			num += count;
			dataReader.releaseHolding();
		}
		System.out.println(" => " + num);
		writer.close();
	}

}
