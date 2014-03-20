package org.shanbo.feluca.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.shanbo.feluca.common.FelucaException;

public class BlockStatus {
	File block;
	Properties statistics;
	int[] offsets;
	
	public BlockStatus(String name){
		try {
			FileInputStream fis = new FileInputStream(name + ".stat");
			statistics = new Properties();
			statistics.load(fis);
			fis.close();
			String offsets = statistics.getProperty("offsets");
			String[] ends = offsets.split(",");
			this.offsets = new int[ends.length];
			for(int i = 0 ; i < ends.length; i ++){
				this.offsets[i] = Integer.parseInt(ends[i]);
			}
			block = new File(name + ".dat");
		} catch (IOException e) {
			throw new FelucaException("status file not found");
		}
		
	}
	
	public int[] getOffsetArray(){
		return Arrays.copyOf(offsets, offsets.length);
	}
	
	//TODO
	public int getNumLines(){
		return 0;
	}

	
}
