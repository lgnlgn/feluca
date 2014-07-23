package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.shanbo.feluca.data.Tuple.AlignColumn;
import org.shanbo.feluca.data.Tuple.TupleType;
import org.shanbo.feluca.data.util.TextReader;

public class TestTextReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("E:/data/ml-10M100K/movielens.txt.test"));
		TextReader tr = new TextReader(br, TupleType.WEIGHT_TYPE, AlignColumn.FIRST);
		long t = System.currentTimeMillis();
		for(String line = tr.readLine(); line!=null; line = tr.readLine() ){
//			System.out.println(line);
		}
		long t2 = System.currentTimeMillis();
		System.out.println(t2 -t);
		tr.close();
	}

}
