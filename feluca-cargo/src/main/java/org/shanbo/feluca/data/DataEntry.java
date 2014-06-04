package org.shanbo.feluca.data;

import java.io.IOException;
import java.util.Properties;

/**
 * entry for {@link DataReader}
 * TODO 
 * @author lgn
 *
 */
public class DataEntry {
	
	private DataReader reader;
	private String dataDir ;
	private boolean inRam;
	
	Properties statistic ;
	
	/**
	 * this construction only fetch statistic of global info
	 * invoke {@link #reOpen()} before reading data!
	 * @param dataDir
	 * @param inRam
	 * @throws IOException
	 */
	public DataEntry(String dataDir, boolean inRam) throws IOException{
		this.dataDir = dataDir;
		this.inRam  = inRam;
		String[] dataDirs =  dataDir.split("/|\\");
		statistic = BlockStatus.loadStatistic(dataDir + "/" +dataDirs[dataDir.length()-1] + ".sta");
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
}
