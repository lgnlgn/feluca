package org.shanbo.feluca.data;

import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.util.BytesUtil;

public abstract class DataReader {
	byte[] inMemData; //a global data set of just a cache reference 
	
	long[] vectorOffsets; //
	int offsetSize;
	Vector vector;
	String dirName;
	
	protected long getOffsetsByIdx(int index){
		if (index < 0 ||  index >= offsetSize){
			throw new FelucaException("offsets index out of range", new IndexOutOfBoundsException());
		}else{
			return vectorOffsets[index];
		}
	}
	
	protected abstract Vector getVectorByOffset(long offset, VectorType type);
	
	public long[] getOffsetArray(){
		long[] tmp = new long[offsetSize];
		System.arraycopy(vectorOffsets, 0, tmp, 0, offsetSize);
		return tmp;
	}
	
	public abstract boolean hasNext();
	
	protected DataReader(String dataName) {
		
		this.dirName = dataName;
		
	}
	
	protected void readOffsetsFromCache(){
		int currentStart = 0;
		int i = 0;
		for(int length = BytesUtil.getInt(inMemData, currentStart); length != 0; currentStart += (length + 4)){
			long offset = ((long)(currentStart + 4) << 32) | ((long)length);
			vectorOffsets[i++] = offset;
			if (i > vectorOffsets.length){
				long[] tmp = new long[vectorOffsets.length + 64 * 1024];
				System.arraycopy(vectorOffsets, 0, tmp, 0, vectorOffsets.length);
				vectorOffsets = tmp;
			}
		}
		offsetSize = i;
	}

	
	
	public void shuffle(){
		CollectionUtil.shuffle(vectorOffsets, 0, offsetSize);
	}
	
	
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
		public void shuffle() {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected Vector getVectorByOffset(long offset, VectorType type) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean hasNext() {
			return false;
		}
		
	}
	
	public static class FSCacheDataReader extends DataReader{
		
		
		long[] currentOffsets;
		DataBuffer fileBuffer;
		
		int vectorIdxOfCurrentCache;
		
		protected FSCacheDataReader(String dataName) {
			super(dataName);
			fileBuffer = new DataBuffer(dirName);
			String vectorType = fileBuffer.getCurrentBlockStatus().getValue("vectorType", "FID_WEIGHT");
			vector = Vector.build(VectorType.valueOf(vectorType));
		}



		@Override
		protected Vector getVectorByOffset(long offset, VectorType type) {
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
