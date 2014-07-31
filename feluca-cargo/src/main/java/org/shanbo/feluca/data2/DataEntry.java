package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.IOException;


public class DataEntry implements Closeable{
	
	VectorReader reader;
	String dataName;
	
	public DataEntry(String dataName){
		this.dataName = dataName;
	}
	
	public void reOpen() throws IOException{
		close();
		reader = new VectorReader(dataName);
	}
	
	public Vector getNextVector() throws IOException{
		return reader.getNextVector();
	}
	
	public void close() throws IOException{
		if (reader!=null)
			reader.close();
	}
	
}
