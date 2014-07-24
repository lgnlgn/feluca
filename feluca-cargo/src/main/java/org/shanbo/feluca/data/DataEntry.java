package org.shanbo.feluca.data;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data.DataReader.RAMDataReader;
import org.shanbo.feluca.data.convert.DataStatistic;


/**
 * <p>entry for {@link DataReader}
 * <p>2 ways of getting statistics of data
 * <p> 1: {@link #getDataReader().getStatistic}   <b>BlockStatus, you must call reOpen first</b>
 *     
 * <p> 2:  {@link #getDataStatistic()}    <b>Properties.</b>
 * 
 * @author lgn
 *
 */
public class DataEntry {

	protected DataReader reader;
	protected String dataDir ;
	protected boolean inRam;

	private long[] offsetArray ;
	private int offsetArrayIdx = Integer.MAX_VALUE;

	protected Properties statistic ;

	protected int currentPosition;
	/**
	 * this construction only fetch statistic of global info
	 * invoke {@link #reOpen()} before reading data!
	 * @param dataDir with absolute path
	 * @param inRam  only false now //TODO true
	 * @throws IOException
	 */
	public DataEntry(String dataDir, boolean inRam) throws IOException{
		this.dataDir = dataDir;
		this.inRam  = inRam;

		statistic = BlockStatus.loadStatistic(dataDir + "/" +new File(dataDir).getName() + ".sta");
	}

	public DataReader getDataReader(){
		return reader;
	}

	/**
	 * create a new {@link DataReader}
	 * @throws IOException
	 */
	public void reOpen() throws IOException{
		
		if (inRam == false){
			reader = DataReader.createDataReader(false, dataDir);
			offsetArray = new long[]{};
			offsetArrayIdx = Integer.MAX_VALUE;
		}else{
			if (reader == null){
				currentPosition = 0;
				offsetArray = new long[]{};
				offsetArrayIdx = Integer.MAX_VALUE;
				reader = DataReader.createDataReader(true, dataDir);
			}
		}
	}

	public void close() throws IOException{
		reader.close();
	}
	
	
	public Properties getDataStatistic(){
		Properties p = new Properties();
		p.putAll(statistic);
		return p;
	}

	/**
	 * sugar for vector access
	 * @return
	 */
	public synchronized Vector getNextVector(){
		if (offsetArrayIdx >= offsetArray.length){
			if (offsetArrayIdx != Integer.MAX_VALUE) // no release on first opening
				reader.releaseHolding();
			if (reader.hasNext()){
				offsetArray = reader.getOffsetArray();
				offsetArrayIdx = 0;
			}else{
				return null;
			}
		}
		Vector v = reader.getVectorByOffset(offsetArray[offsetArrayIdx]);
		offsetArrayIdx += 1;
		currentPosition += 1;
		return v;
	}

	/**
	 * random accessible DataEntry
	 * TODO more test
	 * @author lgn
	 *
	 */
	public static class RADataEntry extends DataEntry{

		int[][] forwardIndex;//[[blockIter, offsetStart, offsetEnd], [], []...]  vector id = index;
		
		public RADataEntry(String dataDir) throws IOException {
			super(dataDir, true);
			super.reOpen();
			forwardIndex = new int[Integer.parseInt(statistic.getProperty(DataStatistic.MAX_VECTOR_ID)) + 1][];
			buildforwardIndex();
			super.close();
		}

		public void reOpen() throws IOException{
			((RAMDataReader) reader).reOpen();
		}

		private void buildforwardIndex(){
			
			RAMDataReader ram = (RAMDataReader) reader;
			while(ram.hasNext()){
				long[] offsetArray2 = ram.getOffsetArray();
				for(int o = 0 ;o < offsetArray2.length; o++){
					Vector v = ram.getVectorByOffset(offsetArray2[o]);
					int start = (int)(((offsetArray2[o] & 0xffffffff00000000l) >> 32) & 0xffffffff);
					int end = (int)(offsetArray2[o] & 0xffffffffl);

					forwardIndex[v.getIntHeader()] = new int[]{ram.getBlockIter(), start, end};
				}
				ram.releaseHolding();
			}
		}
		
		
		/**
		 * the Vector must have an ID
		 * @param vectorId
		 * @param deepIndex
		 * @return
		 */
		public synchronized Vector getVectorById(int vectorId){
			int[] index = forwardIndex[vectorId];
			return ((RAMDataReader) reader).getVectorOfBlock(index[0], index[1], index[2]);
		}
	}
	
}
