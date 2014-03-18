package org.shanbo.feluca.data;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class DataReader<T> {
	byte[] cache; 
	long[] dataOffsets;
	int numData;
	int incompleteStartOffset;
	protected FileInputStream fis = null;
	protected InputStreamReader reader ;
	
	
	public void init() {
		
	}
	
	public void shuffle(){
		
	}
	
	public synchronized T getOne(){
		return null;
	}
	
	public void close(){
		
	}
	
}
