package org.shanbo.feluca.data2;

import java.io.IOException;

public class TestShuffled {

	public static void main(String[] args) throws Exception {
		ShuffledDataEntry de = new ShuffledDataEntry("data/mltrain", 20000);
		System.out.println("!!!");
		de.reOpen();
		int count = 0;
		for(Vector v = de.getNextVector(); v!= null ; v = de.getNextVector()){
			count +=1;
		}
		System.out.println(count);
		de.close();
	}

}
