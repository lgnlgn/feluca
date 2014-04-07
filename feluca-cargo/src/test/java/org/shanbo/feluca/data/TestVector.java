package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileReader;

import org.shanbo.feluca.data.Vector.VectorType;

public class TestVector {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("E:/data/rcv1.txt"));
		Vector a = Vector.build(VectorType.LABEL_FID_WEIGHT);
		int i = 0;
		for(String line = br.readLine(); line!= null; line = br.readLine(), i++){
			a.parseLine(line);
			if (i < 20){
				System.out.println(a.toString());
			}
		}
		br.close();
		System.out.println("   " + i);
	}

}
