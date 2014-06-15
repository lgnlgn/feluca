package org.shanbo.feluca.significanttesting;

import java.io.IOException;

import org.shanbo.feluca.data.DataEntry;


public abstract class RandomSwapper {

	DataEntry input;
	String outputPrefix;
	
	int itersPerLoop;
	int loops;
	
	public RandomSwapper(String inputData, String outputPrefix,int itersPerLoop, int loops) throws IOException{
		this.input = new DataEntry(inputData, false);
		this.outputPrefix = outputPrefix;
		this.itersPerLoop = itersPerLoop;
		this.loops = loops;
	}
	
	abstract public void runSwap()throws IOException;
}
