package org.shanbo.feluca.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.util.BytesUtil;

import com.google.common.io.Closeables;

/**
 * General vector reader. use {@link #createDataReader(boolean, String)} to fetch an instance
 * <p><b>DO NOT support random access! </b>
 * <p>
 * 
 * <p>while(reader.hasNext()){ <p>
 * <p>&nbsp;&nbsp;   long[] offsetArray = reader.getOffsetArray();<p>
 * <p>&nbsp;&nbsp;   for(int o = 0 ; o < offsetArray.length; o++){
 * <p>&nbsp;&nbsp;&nbsp;		Vector v = reader.getVectorByOffset(offsetArray[o]);
 * <p>&nbsp;&nbsp;&nbsp;					//<i> do your computation</b>
 * <p>&nbsp;&nbsp;		}
 * <p>&nbsp;<b> reader.releaseHolding() </b>
 * <p>&nbsp;}
 * <p>&nbsp;
 * @author lgn
 *
 */
public abstract class DataReader implements Closeable {
	byte[] inMemData; //a global data set of just a cache reference 

	long[] vectorOffsets; //automatically enlarge
	int offsetSize;
	Vector vector;
	String dirName;


	public abstract Vector getVectorByOffset(long offset);

	public long[] getOffsetArray(){
		return Arrays.copyOf(vectorOffsets, offsetSize);
	}

	/**
	 * call it after hasNext(). Usually inside the end of a hasNext()_loop
	 */
	public abstract void releaseHolding();

	/**
	 * call it before iterate vectors
	 * @return
	 */
	public abstract boolean hasNext();


	public abstract void close() throws IOException;
	
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


	public static DataReader createDataReader(boolean inRAM, String dataName) throws IOException{
		if (inRAM){
			return new RAMDataReader(dataName);
		}else{
			return new FSCacheDataReader(dataName);
		}


	}

	/**
	 * support random access; read all blocks of data into memory
	 * TODO more tests need
	 * @author lgn
	 *
	 */
	public static class RAMDataReader extends FSCacheDataReader{
		
		int blockIter; // trace of blocks

		List<byte[]> dats;
		List<long[]> offsets;
		List<BlockStatus> blockStatuses;
		//TODO
		private RAMDataReader(String dataName) throws IOException {
			super(dataName);
			dats = new ArrayList<byte[]>();
			offsets = new ArrayList<long[]>();
			while(super.hasNext()){
				dats.add(Arrays.copyOfRange(inMemData, 0, inMemData.length));
				offsets.add(Arrays.copyOfRange(vectorOffsets, 0, vectorOffsets.length));
				super.releaseHolding();
			}
			blockStatuses = fileBuffer.blocks;
		}

		/**
		 * for random access
		 * @param blockIter
		 * @param start
		 * @param end
		 * @return
		 */
		public Vector getVectorOfBlock(int blockIter, int start, int end){
			vector.set(dats.get(blockIter), start, end);
			return vector;
		}
		
		
		public int getBlocks(){
			return dats.size();
		}

		/**
		 * for random access
		 * @param blockIdx
		 * @param start
		 * @param end
		 * @return
		 */
		public Vector getVectorOfBlock(int blockIdx, long offset){
			int start = (int)(((offset & 0xffffffff00000000l) >> 32) & 0xffffffff);
			int end = (int)(offset & 0xffffffffl);
			vector.set(dats.get(blockIdx), start, end);
			return vector;
		}
		
		
		@Override
		public Vector getVectorByOffset(long offset) {
			int start = (int)(((offset & 0xffffffff00000000l) >> 32) & 0xffffffff);
			int end = (int)(offset & 0xffffffffl);
			return getVectorOfBlock(blockIter, start, end);
		}

		@Override
		public boolean hasNext() {
			if ((blockIter+1) < dats.size()){
				return true;
			}
			return false;
		}

		public synchronized void reOpen(){
			blockIter = 0;
		}
		
		public int getBlockIter(){
			return blockIter;
		}
		
		public long[] getOffsetArray(){
			return Arrays.copyOfRange(offsets.get(blockIter), 0 , offsets.get(blockIter).length);
		}
		
		@Override
		public void releaseHolding() {
			++blockIter;
		}
		
		@Override
		public void close() {
		}

	}

	public static class FSCacheDataReader extends DataReader{

		DataBuffer fileBuffer;


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



		@Override
		public void close() throws IOException {
			Closeables.close(fileBuffer, false);
		}

	}

}
