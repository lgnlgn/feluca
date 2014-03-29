package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

public class TestReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		BufferedReader reader = new BufferedReader(new FileReader("E:/data/real-sim"));
//		int i = 0;
//		long t = System.nanoTime();
//		for(String line = reader.readLine(); line!= null; line = reader.readLine()){	
//			i+=1;
//			if (i<10){
//				System.out.println(line);;
//			}
//		}
//		long t2 = System.nanoTime();
//		System.out.println(t2-t);
		byte[] b = new byte[32 * 1024 * 1024];
		long t = System.nanoTime();
 		FileInputStream fs = new FileInputStream("E:/data/kaggle_visible_evaluation_triplets.txt");
		int read = fs.read(b);
		System.out.println("---" + read);
		read = fs.read(b);
		System.out.println("---" + read);
		read = fs.read(b);
		System.out.println("---" + read);
		fs.close();
		long t2 = System.nanoTime();
		System.out.println(t2-t);
	}

}
