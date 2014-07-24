package org.shanbo.feluca.data;

import java.io.IOException;

import org.shanbo.feluca.data.DataEntry.RADataEntry;
import org.shanbo.feluca.data.DataReader.RAMDataReader;

public class TestRamEntry {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		RADataEntry ra = new RADataEntry("data/movielens_test");
		ra.reOpen();
		int i =0;
		for(Vector v = ra.getNextVector();v!=null;v =ra.getNextVector()){
			i+=1;
		}
		System.out.println(i);
		
		System.out.println(ra.getVectorById(1256));
		System.out.println(ra.getVectorById(4538));
		System.out.println(ra.getVectorById(25364));
		ra.close();
	}

}
