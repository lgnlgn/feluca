package org.shanbo.feluca.significanttesting;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;
import org.shanbo.feluca.paddle.common.collection.IntArray;

/**
 * self-loop approach for ARISTIDES GIONIS's binary swap randomization
 * see "Accessing data mining result via swap randomization"
 * TODO tests
 * @author lgn
 *
 */
public class BigraphRandomizeSwapper extends RandomSwapper{
	public BigraphRandomizeSwapper(String inputData, String outputPrefix,
			int itersPerLoop, int loops) throws IOException {
		super(inputData, outputPrefix, itersPerLoop, loops);
	}

	final static String outputSuffix = ".dat";
	HashSet<Long> graph = new HashSet<Long>();
	
	IntArray jref ; 
	IntArray iref ;

	Random r = new Random();
	int n = 0; // total # of elements
	
	

	public int swap(){
		int a = r.nextInt(n);
		int b = r.nextInt(n);
		int aj = jref.get(a);
		int ai = iref.get(a);
		int bj = jref.get(b);
		int bi = iref.get(b);
		if (!graph.contains(  ((long)aj << 32)|(long)(bi) ) && !graph.contains( ((long)bj << 32)|(long)(ai))){
			graph.remove(((long)aj << 32)|(long)(ai));
			graph.remove(((long)bj << 32)|(long)(bi));
			graph.add(((long)aj << 32)|(long)(bi));
			graph.add(((long)bj << 32)|(long)(ai));
			iref.set(a, bi);
			iref.set(b, ai);
			return 1;
		}
		return 0;
	}
	
	@Override
	public void runSwap() throws IOException {
		int attributes = 1024 ; //for the capacity of IntegerArray initialization
		long t = 0;
		attributes = Math.max(attributes, Utilities.getIntFromProperties(input.getDataStatistic(), DataStatistic.MAX_FEATURE_ID));
		
		jref = new IntArray(attributes);
		iref = new IntArray(attributes);
		
		int rowc = 0;
		int maxfeatureSize = 0;
		System.out.println("loading data~");
		input.reOpen();
		
		Vector sample = input.getNextVector();
		for(; sample!=null; sample = input.getNextVector()){
			for( int i = 0 ; i < sample.getSize(); i++){
				jref.add(rowc);
				iref.add(sample.getFId(i));
				graph.add(((long)rowc << 32) | ((long)sample.getFId(i)));
				n += 1;
			}
			rowc += 1;
			maxfeatureSize = Math.max(maxfeatureSize, sample.getSize());
		}
		
		System.out.println("loading data finished!  " );

		Integer[] newFidArray = new Integer[maxfeatureSize];
		int swaps = 0;
		int total = itersPerLoop * loops;

		// starting swap & output
		for(int i = 0 ; i < total; i++){
			if (i % itersPerLoop == 0){ 
				int k = 0; //rowid
				BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("%s.%d%s", outputPrefix, (i/itersPerLoop), outputSuffix)));
				int idx = 0;
				for (int l = 0; l<= n; l++){
					if (l < n && (k == jref.get(l))){
						newFidArray[idx] = iref.get(l); // feature id
						idx ++ ;
					}else{
//						row.featureSize = idx;
						Arrays.sort(newFidArray, 0, idx);
//						writer.write(row); // write out a vector
						writer.write(StringUtils.join(newFidArray, " ", 0, idx));
						
						if (l < n){
							idx = 0;
							newFidArray[idx++] = iref.get(l);
							k = jref.get(l);
						}
					}
				}
				writer.close();
				
				if (i > 0){
					long t2 = System.currentTimeMillis();
					
					System.out.println(String.format("%.1f\t%d\t%d\t%.4f\t%.5f", 
							(t2-t)/1000.0, i, swaps, (swaps + 0.0)/i, (swaps + 0.0)/n));
					t = t2;
				}else{
					//first time 
					System.out.println("time(s)\titerlen\tswapped\t%/loop\t%/all");
					System.out.println("0.0\t0\t0\t0.0\t0.0");
					t = System.currentTimeMillis();
				}
			}
			swaps += swap();
		}
		
	}
	




}
