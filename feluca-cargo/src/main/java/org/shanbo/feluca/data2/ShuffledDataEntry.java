package org.shanbo.feluca.data2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * buffered 
 * @author lgn
 *
 */
public class ShuffledDataEntry extends DataEntry{

	List<Vector> cache ;
	int iter ;
	final int maxVectorSize;

	public ShuffledDataEntry(String dataName, int vectorSize) throws IOException {
		super(dataName, true);
		if (vectorSize <= 0){
			throw new RuntimeException("vectorSize <= 0");
		}
		this.maxVectorSize = vectorSize;
		cache = new ArrayList<Vector>(this.maxVectorSize);
	}

	public ShuffledDataEntry(String dataName) throws IOException {
		this(dataName, 2000000);
	}

	public synchronized Vector getNextVector() throws Exception{
		if (iter >= cache.size() || cache.isEmpty()){
			cache.clear();
			for(int i = 0 ; i < maxVectorSize;i++){
				Vector v = super.getNextVector();
				if (v != null)
					cache.add(v);
				else {
					super.close();
					break;
				}
			}
			if (cache.isEmpty()){
				return null;
			}
			Collections.shuffle(cache);
			iter = 0;
		}
		Vector current = cache.get(iter);
		iter += 1;
		return current;
	}


	public synchronized void reOpen() throws Exception{
		super.close();
		reader = new SeqVectorReader(dataName, pattern, true);
		cache.clear();
		iter = 0;
	}

}
