package org.shanbo.feluca.data;

import java.util.List;
import java.util.NoSuchElementException;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.hash.TIntFloatHashMap;

public class PartialModel {

	final static int MODEL_CLEAR_INTERVAL = 4;

	TIntFloatHashMap model;
	TIntFloatHashMap tmp;
	Partitioner partitioner;
	int numModelCleared = 0;
	public static interface Partitioner{
		public int allocate(int id);
	}

	/**
	 * TODO
	 * @param bytesParks
	 */
	public void partitionAndSerialize(int[] ids, List<BytesPark> bytesParks){
		for(int i = 0 ; i < ids.length; i++){
			float oldValue = model.get(ids[i]);
			float newValue = tmp.get(ids[i]);
			int allocate = partitioner.allocate(ids[i]);
			BytesPark bytesPark = bytesParks.get(allocate);
			bytesPark.swallow(ids[i], newValue - oldValue);
		}
	}

	/**
	 * TODO
	 * @param bytesParks
	 */
	public void deserializeFrom(List<BytesPark> bytesParks){
		numModelCleared += 1;
		if (numModelCleared > MODEL_CLEAR_INTERVAL){
			model.clear();
			tmp.clear();
		}
		for(BytesPark bytesPark: bytesParks){
			for(int offset = 0 ; offset < bytesPark.arraySize(); offset += 8){
				int id = bytesPark.yieldIdFrom8Bytes(offset);
				float value = bytesPark.yieldValueFrom8Bytes(offset);
				model.put(id, value);
				tmp.put(id, value);
			}
		}

	}
}
