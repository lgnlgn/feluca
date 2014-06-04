package org.shanbo.feluca.data;

import java.io.File;
import java.io.IOException;

import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.util.BytesUtil;

/**
 * General vector reader. use {@link #createDataReader(boolean, String)} to fetch an instance
 * <p>
 * 
 * <p>while(reader.hasNext()){ <p>
 * <p>&nbsp;&nbsp;   long[] offsetArray = reader.getOffsetArray();<p>
 * <p>&nbsp;&nbsp;   for(int o = 0 ; o < offsetArray.length; o++){
 * <p>&nbsp;&nbsp;&nbsp;		Vector v = dataInput.getVectorByOffset(offsetArray[o]);
 * <p>&nbsp;&nbsp;&nbsp;					//<i> do your computation</b>
 * <p>&nbsp;&nbsp;		}
 * <p>&nbsp;<b> reader.releaseHolding() </b>
 * <p>&nbsp;}
 * <p>&nbsp;
 * @author lgn
 *
 */
public abstract class DataReader {
	byte[] inMemData; //a global data set of just a cache reference 

	long[] vectorOffsets; //
	int offsetSize;
	Vector vector;
	String dirName;

	BlockStatus glocalStatus;

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

	/**
	 * call it after hasNext(). Usually inside the end of a hasNext()_loop
	 */
	public abstract void releaseHolding();

	public abstract boolean hasNext();

	public BlockStatus getStatistic(){
		return glocalStatus;
	}

	private DataReader(String dataName) {
		this.glocalStatus = new BlockStatus(new File(dataName));
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


		@Override
		public void releaseHolding() {
			// TODO Auto-generated method stub

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



		@Override
		public void releaseHolding() {
			fileBuffer.releaseByteArrayRef();
		}

	}

}
