package org.shanbo.feluca.data;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


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
	private String dataDir ;
	private boolean inRam;

	private long[] offsetArray = new long[]{};
	private int offsetArrayIdx = Integer.MAX_VALUE;

	private Properties statistic ;

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
		reader = DataReader.createDataReader(false, dataDir);
		currentPosition = 0;
	}

	public void close() throws IOException{
		reader.close();
	}
	
	
	public Properties getDataStatistic(){
		return new Properties(statistic);
	}

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
	 * 
	 * @author lgn
	 *
	 */
	public static class VDataEntry extends DataEntry{

		long[] forwardIndex;//
		
		public VDataEntry(String dataDir) throws IOException {
			super(dataDir, true);
			//TODO 
		}

		public void reOpen() throws IOException{
			
		}

		/**
		 * the Vector must have an ID
		 * @param vectorId
		 * @param deepIndex
		 * @return
		 */
		public synchronized Vector getVectorById(int vectorId){
			long offset = forwardIndex[vectorId];
			return reader.getVectorByOffset(offset);
		}
	}
	
}
