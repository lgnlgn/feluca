package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileReader;

import org.shanbo.feluca.data.Vector.VectorType;

public class TestVector {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("E:/data/mushroom.dat"));
		Vector a = Vector.build(VectorType.FIDONLY);
		int i = 0;
		for(String line = br.readLine(); line!= null; line = br.readLine(), i++){
			a.parseLine(line);
			System.out.println(a.toString());
		}
		br.close();
		System.out.println("   " + i);
	}

}
