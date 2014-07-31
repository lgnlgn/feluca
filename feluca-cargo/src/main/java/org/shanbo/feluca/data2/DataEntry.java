package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector.VectorType;


public class DataEntry implements Closeable{
	
	VectorReader reader;
	String dataName;
	String pattern ;
	public DataEntry(String dataName){
		this(dataName, "\\.\\d+\\.dat");
	}
	
	public DataEntry(String dataName, String pattern){
		this.dataName = dataName;
		this.pattern = pattern;
	}
	
	public void reOpen() throws IOException{
		close();
		reader = new VectorReader(dataName, pattern);
	}
	
	public Vector getNextVector() throws IOException{
		return reader.getNextVector();
	}
	
	public VectorType getVectorType(){
		return reader.getVectorType();
	}
	
	public Properties getDataStatistic(){
		return reader.getDataStatistic();
	}
	
	public void close() throws IOException{
		if (reader!=null)
			reader.close();
	}
	
	public static DataEntry createDataEntry(String dataName, boolean inRam){
		if (inRam){
			return new RADataEntry(dataName);
		}else {
			return new DataEntry(dataName);
		}
	}
	
	public static DataEntry createDataEntry(String dataName, String pattern, boolean inRam){
		if (inRam){
			return new RADataEntry(dataName, pattern);
		}else {
			return new DataEntry(dataName, pattern );
		}
	}
	
	
	public static class RADataEntry extends DataEntry{

		Vector[] vectors;
		int idx = 0;
		
		public RADataEntry(String dataName){
			super(dataName);
		}
		
		public RADataEntry(String dataName, String pattern){
			super(dataName, pattern);
		}
		
		public Vector getVectorById(int vectorId){
			return vectors[vectorId];
		}
		
		public Vector getNextVector(){
			Vector v;
			for(v = vectors[idx]; v == null ; idx += 1){
			}
			return v;
		}
		
		public void reOpen() throws IOException{
			if (vectors == null){
				super.reOpen();
				vectors = new Vector[
				                     Integer.parseInt(reader.getDataStatistic().getProperty(DataStatistic.MAX_VECTOR_ID)) + 1];
				
				for(Vector v = super.getNextVector(); v!= null; v = super.getNextVector()){
					vectors[v.getIntHeader()] = v;
				}
				super.close();
			}
			idx = 0;
		}
		
	}
	
}
