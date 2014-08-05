package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector.VectorType;

/**
 * sugar for VectorReader
 * TODO add a queue for producer/consumer mode
 * @author lgn
 *
 */
public class DataEntry implements Closeable{
	
	VectorReader reader;
	String dataName;
	String pattern ;
	public DataEntry(String dataName) throws IOException{
		this(dataName, "\\.\\d+\\.dat");
	}
	
	public DataEntry(String dataName, String pattern) throws IOException{
		this.dataName = dataName;
		this.pattern = pattern;
		reader = new SeqVectorReader(dataName, pattern);
	}
	
	public void reOpen() throws IOException{
		close();
		reader = new SeqVectorReader(dataName, pattern);
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
	
	public static DataEntry createDataEntry(String dataName, boolean inRam) throws IOException{
		if (inRam){
			return new RAMDataEntry(dataName);
		}else {
			return new DataEntry(dataName);
		}
	}
	
	public static DataEntry createDataEntry(String dataName, String pattern, boolean inRam) throws IOException{
		if (inRam){
			return new RAMDataEntry(dataName, pattern);
		}else {
			return new DataEntry(dataName, pattern );
		}
	}
	
	
	public static class RAMDataEntry extends DataEntry{

		Vector[] vectors;
		int idx = 0;
		
		public RAMDataEntry(String dataName) throws IOException{
			super(dataName);
		}
		
		public RAMDataEntry(String dataName, String pattern) throws IOException{
			super(dataName, pattern);
		}
		
		public Vector getVectorByIndex(int index){
			return vectors[index];
		}
		
		public Vector getNextVector(){
			return vectors[idx++];
		}
		
		public void reOpen() throws IOException{
			if (vectors == null){
				super.reOpen();
				vectors = new Vector[
				                     Integer.parseInt(reader.getDataStatistic().getProperty(DataStatistic.NUM_VECTORS)) + 1];
				idx = 0;
				for(Vector v = super.getNextVector(); v!= null; v = super.getNextVector()){
					vectors[idx++] = v;
				}
				super.close();
			}
			idx = 0;
		}
		
	}
	
}
