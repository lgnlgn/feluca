package org.shanbo.feluca.data2;

import java.io.IOException;


public class TestRAMEntry {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws Exception {
		DataEntry de =  DataEntry.createDataEntry("data/real-sim", true);
		System.out.println("!!!");
		de.reOpen();
		int count = 0;
		for(Vector v = de.getNextVector(); v!= null ; v = de.getNextVector()){
			count +=1;
		}
		System.out.println(count);
	}

}
