package org.shanbo.feluca.cf.common;

import java.io.IOException;

import org.shanbo.feluca.data.DataEntry;
import org.shanbo.feluca.data.Vector;


public class CFDataEntry extends DataEntry{

	public static class VectorBag{
		public Vector vector;
		public int position;
	}
	
	public CFDataEntry(String dataDir, boolean inRam) throws IOException {
		super(dataDir, inRam);
	}


	/**
	 * vector have an ID
	 * @param vectorId
	 * @param deepIndex
	 * @return
	 */
	public synchronized VectorBag getVectorByDeepIndex(int vectorId, int deepIndex){
		return null;
		//TODO
	}
}
