package org.shanbo.feluca.data2;

import java.io.IOException;

public class TestVectorReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	
		SeqVectorReader vr = new SeqVectorReader("data/mltrain");
		int count = 0;
		for(Vector v = vr.getNextVector(); v!= null; v = vr.getNextVector()){
			count ++;
			if (count < 10){
				System.out.println(v);
			}
		}
		System.out.println(count);
	}

}
