package org.shanbo.feluca.data2;

import java.io.IOException;

public class TestMultiReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		MultiVectorReader reader = new MultiVectorReader("data/mltrain", null);
		int count =0 ;
		for(Vector v  = reader.getNextVector(); v!= null  ;v = reader.getNextVector()){
			count ++;
			if (count < 20){
				System.out.println(v.toString());
			}
		}
		System.out.println(count);
		reader.close();
	}

}
