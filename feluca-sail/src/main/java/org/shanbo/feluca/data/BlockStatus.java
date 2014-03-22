package org.shanbo.feluca.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.shanbo.feluca.common.FelucaException;

public class BlockStatus {
	File block;
	Properties statistics;
	int[] offsets;
	
	public BlockStatus(String prefix){
		try {
			FileInputStream fis = new FileInputStream(prefix + ".stat");
			statistics = new Properties();
			statistics.load(fis);
			fis.close();
			String offsets = statistics.getProperty("offsets");
			String[] ends = offsets.split(",");
			this.offsets = new int[ends.length];
			for(int i = 0 ; i < ends.length; i ++){
				this.offsets[i] = Integer.parseInt(ends[i]);
			}
			//TODO whether offsets are relative or absolute
			for(int i = ends.length -1 ; i > 0 ; i--){
				this.offsets[i] = this.offsets[i] - this.offsets[i-1];
			}
			block = new File(prefix + ".dat");
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
