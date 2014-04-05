package org.shanbo.feluca.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.DataStatistic.BasicStatistic;
import org.shanbo.feluca.data.DataStatistic.MinStatistic;;
public class TestStatistic {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("E:/data/mushroom.dat"));
		
		Vector a = Vector.build(VectorType.FIDONLY);
		DataStatistic ds = new MinStatistic(new BasicStatistic());
		int i = 0;
		for(String line = br.readLine(); line!= null; line = br.readLine(), i++){
			a.parseLine(line);
			ds.stat(a);
		}
		br.close();
		System.out.println("   " + i);
		System.out.println(ds.toString());
	}
}
