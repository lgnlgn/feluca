package org.shanbo.feluca.data;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public abstract class DataReader {
	byte[] inMemData; //a global data set of just a cache reference 
	
	long[] vectorOffsets; //

	int currentVectorIdxOfCache;
	
	
	protected DataReader(String dataName) {
		
	}
	
	
	protected abstract void init();
	
	protected abstract void shuffle();
	
	
	public static DataReader createDataReader(boolean inRAM, String dataName){
		if (inRAM){
			return new RAMDataReader(dataName);
		}else{
			return new FSCacheDataReader(dataName);
		}
		
		
	}
	
	public static class RAMDataReader extends DataReader{

		protected RAMDataReader(String dataName) {
			super(dataName);
		}

		@Override
		public void init() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void shuffle() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class FSCacheDataReader extends DataReader{

		protected FSCacheDataReader(String dataName) {
			super(dataName);
		}

		@Override
		public void init() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void shuffle() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
}
