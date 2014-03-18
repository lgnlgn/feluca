package org.shanbo.feluca.data;

import java.util.Arrays;
import java.util.Properties;

public class BlockStatus {
	String blockPath;
	Properties statistics;
	int[] offsets;
	
	public int[] getOffsetArray(){
		if (offsets == null){
			//TODO
		}
		return Arrays.copyOf(offsets, offsets.length);
	}
	
	//TODO
	public int getNumLines(){
		return 0;
	}

	
}
