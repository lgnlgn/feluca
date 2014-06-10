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

	private DataReader reader;
	private String dataDir ;
	private boolean inRam;

	private long[] offsetArray = new long[]{};
	private int offsetArrayIdx = Integer.MAX_VALUE;

	private Properties statistic ;

	
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
	}

	public Properties getDataStatistic(){
		return new Properties(statistic);
	}

	public synchronized Vector getNextVector(){
		if (offsetArrayIdx >= offsetArray.length){
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
		return v;
	}

}
