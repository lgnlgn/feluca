package org.shanbo.feluca.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data.convert.DataStatistic;

/**
 * statistic of block; 
 * to get statistic of full data; use {@link #BlockStatus(File)} without data block file
 * to get statistic of a block ; use {@link #BlockStatus(String)}
 * @author lgn
 *
 */
public class BlockStatus {
	File block;
	Properties statistics;
	int blockSize;
	public BlockStatus(String prefix){
		try {
			statistics = loadStatistic(prefix + ".sta");
			block = new File(prefix + ".dat");
			blockSize = (int)block.length();
		} catch (IOException e) {
			throw new RuntimeException("status file not found");
		}

	}
	
	
	public BlockStatus(File dataName){
		String fullPath = dataName.getAbsolutePath() + "/" + dataName.getName();
		try {
			statistics = loadStatistic(fullPath + ".sta");
			File[] files = dataName.listFiles();
			block = null;
			for(File f : files){
				if (f.getName().endsWith(".dat")){
					blockSize += (int )f.length();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("status file not found");
		}
	}

	public int getBlockSize(){
		return blockSize;
	}

	public int getNumLines(){
		return Integer.parseInt(DataStatistic.NUM_VECTORS);
	}

	public int getValue(String key, int defaultValue){
		try{
			return Integer.parseInt(statistics.getProperty(key));
		}catch (Exception e) {
			return defaultValue;
		}
	}

	public String getValue(String key, String defaultValue){

		if ( statistics.getProperty(key) == null)
			return defaultValue;
		else {
			return  statistics.getProperty(key);
		}

	}


	public double getDouble(String key, double defaultValue){
		try{
			return Double.parseDouble(statistics.getProperty(key));
		}catch (Exception e) {
			return defaultValue;
		}
	}

	public String getString(String key, String defaultValue){
		return  statistics.getProperty(key, defaultValue);
	}
	
	public int getInt(String key, int defaultValue){
		try{
			return Integer.parseInt(statistics.getProperty(key));
		}catch (Exception e) {
			return defaultValue;
		}
	}
	
	public long getLong(String key, long defaultValue){
		try{
			return Long.parseLong(statistics.getProperty(key));
		}catch (Exception e) {
			return defaultValue;
		}
	}
	
	public static Properties loadStatistic(String filePath) throws IOException{
		FileInputStream fis = new FileInputStream(filePath);
		Properties p = new Properties();
		p.load(fis);
		fis.close();
		return p;
	}
}
