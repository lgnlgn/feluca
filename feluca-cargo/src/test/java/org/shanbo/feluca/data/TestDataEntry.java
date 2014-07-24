package org.shanbo.feluca.data;

import java.io.IOException;
import java.util.Iterator;

public class TestDataEntry {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DataEntry de = new DataEntry("data/movielens_train", false);
		de.reOpen();
		int i = 0;
		for(Vector v = de.getNextVector(); v!= null; v = de.getNextVector()){
//			if (i > 69870){
//				System.out.println(v.toString());
//			}
			i += 1;
			
		}
		System.out.println(i);
	}

}
