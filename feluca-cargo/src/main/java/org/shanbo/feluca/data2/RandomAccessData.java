package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data2.DataEntry.RAMDataEntry;

public class RandomAccessData implements Closeable{

	RAMDataEntry ramDataEntry ;
	int[] vidIndex; //forward-index
	
	public RandomAccessData(String dirName) throws IOException{
		this( (RAMDataEntry)(DataEntry.createDataEntry(dirName, true)));
	}

	public RandomAccessData(RAMDataEntry ramDataEntry){
		this.ramDataEntry = ramDataEntry;
		Properties stat = ramDataEntry.getDataStatistic();
		vidIndex = new int[Integer.parseInt(stat.getProperty(DataStatistic.MAX_VECTOR_ID)) + 1];
		int i = 0;
		for(Vector v = ramDataEntry.getNextVector(); v!= null; v = ramDataEntry.getNextVector()){
			vidIndex[v.getIntHeader()] = i++;
		}
	}
	
	public Vector getVectorById(int vectorId){
		return ramDataEntry.getVectorByIndex(vidIndex[vectorId]);
	}
	
	public void close() throws IOException{
		ramDataEntry.close();
	}
}
