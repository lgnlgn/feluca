package org.shanbo.feluca.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.common.FelucaException;

public class BlockStatus {
	File block;
	Properties statistics;
	int blockSize;
	public BlockStatus(String prefix){
		try {
			FileInputStream fis = new FileInputStream(prefix + ".stat");
			statistics = new Properties();
			statistics.load(fis);
			fis.close();
			blockSize = Integer.parseInt(statistics.getProperty("blockSize"));

			block = new File(prefix + ".dat");
		} catch (IOException e) {
			throw new FelucaException("status file not found");
		}

	}

	public int getBlockSize(){
		return blockSize;
	}

	//TODO
	public int getNumLines(){
		return 0;
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

}
