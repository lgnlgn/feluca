package org.shanbo.feluca.data;

import java.io.IOException;

import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.util.BytesUtil;
import org.shanbo.feluca.data.util.CollectionUtil;

/**
 * General vector reader. use {@link #createDataReader(boolean, String)} to fetch an instance
 * @author lgn
 *
 */
public abstract class DataReader {
	byte[] inMemData; //a global data set of just a cache reference 
	
	long[] vectorOffsets; //
	int offsetSize;
	Vector vector;
	String dirName;
	
	public long getOffsetsByIdx(int index){
		if (index < 0 ||  index >= offsetSize){
			throw new RuntimeException("offsets index out of range", new IndexOutOfBoundsException());
		}else{
			return vectorOffsets[index];
		}
	}
	
	public abstract Vector getVectorByOffset(long offset);
	
	public long[] getOffsetArray(){
		long[] tmp = new long[offsetSize];
		System.arraycopy(vectorOffsets, 0, tmp, 0, offsetSize);
		return tmp;
	}
	
	public abstract boolean hasNext();
	
	private DataReader(String dataName) {
		
		this.dirName = dataName;
		this.vectorOffsets = new long[32 * 1024];
	}
	
	protected void readOffsetsFromCache(){
		int currentStart = 0;
		int i = 0;
		for(int length = BytesUtil.getInt(inMemData, currentStart); length != 0; ){
			long offset = ((long)(currentStart + 4) << 32) | ((long)(currentStart + 4 + length));
			vectorOffsets[i++] = offset;
			if (i >= vectorOffsets.length){
				long[] tmp = new long[vectorOffsets.length + 64 * 1024];
				System.arraycopy(vectorOffsets, 0, tmp, 0, vectorOffsets.length);
				vectorOffsets = tmp;
			}
			//for next loop
			currentStart += (length + 4);
			length = BytesUtil.getInt(inMemData, currentStart);
		}
		offsetSize = i;
	}

	
	
	public void shuffle(){
		CollectionUtil.shuffle(vectorOffsets, 0, offsetSize);
	}
	
	
	public static DataReader createDataReader(boolean inRAM, String dataName) throws IOException{
		if (inRAM){
			return new RAMDataReader(dataName);
		}else{
			return new FSCacheDataReader(dataName);
		}
		
		
	}
	
	public static class RAMDataReader extends DataReader{

		private RAMDataReader(String dataName) {
			super(dataName);
		}


		@Override
		public Vector getVectorByOffset(long offset) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean hasNext() {
			return false;
		}
		
	}
	
	public static class FSCacheDataReader extends DataReader{

		DataBuffer fileBuffer;

		int vectorIdxOfCurrentCache;
		
		private FSCacheDataReader(String dataName) throws IOException {
			super(dataName);
			fileBuffer = new DataBuffer(dirName);
			String vectorType = fileBuffer.getCurrentBlockStatus().getValue("vectorType", "FID_WEIGHT");
			vector = Vector.build(VectorType.valueOf(vectorType));
			fileBuffer.start();
		}



		@Override
		public Vector getVectorByOffset(long offset) {
			int start = (int)(((offset & 0xffffffff00000000l) >> 32) & 0xffffffff);
			int end = (int)(offset & 0xffffffffl);
			vector.set(inMemData, start, end);
			return vector;
		}

		@Override
		public boolean hasNext() {
			inMemData = fileBuffer.getByteArrayRef();
			if (inMemData == null)
				return false;
			else{
				this.readOffsetsFromCache();
				return true;
			}
		}
		
	}
	
}
