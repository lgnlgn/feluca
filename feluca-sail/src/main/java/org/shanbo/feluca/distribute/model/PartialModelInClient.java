package org.shanbo.feluca.distribute.model;

import java.util.List;
import java.util.NoSuchElementException;

import org.shanbo.feluca.common.FelucaException;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.hash.TIntFloatHashMap;

/**
 * model 
 * @author lgn
 *
 */
public class PartialModelInClient {

	final static int MODEL_CLEAR_INTERVAL = 4;

	TIntFloatHashMap model ;
	TIntFloatHashMap tmp;
	Partitioner partitioner;
	int numModelCleared = 0;
	
	public PartialModelInClient(int totalModelSegments){
		partitioner = new Partitioner.HashPartitioner(totalModelSegments);
	}
	


	/**
	 * ids  to BytesPark  according to partitioner
	 * @param ids
	 * @param bytesParks
	 */
	public void partitionQueryIds(int[] ids, BytesPark[] bytesParks){
		for(int i = 0 ; i < ids.length; i++){
			int allocate = partitioner.decidePartition(ids[i]);
			BytesPark bytesPark = bytesParks[allocate];
			bytesPark.swallowInt(ids[i]);
		}
	}
	
	
	/**
	 * TODO
	 * @param bytesParks
	 */
	public void partitionAndSerialize(int[] ids, BytesPark[] bytesParks){
		for(int i = 0 ; i < ids.length; i++){
			float oldValue = model.get(ids[i]);
			float newValue = tmp.get(ids[i]);
			int allocate = partitioner.decidePartition(ids[i]);
			BytesPark bytesPark = bytesParks[allocate];
			bytesPark.swallow(ids[i], newValue - oldValue);
		}
	}

	/**
	 * TODO
	 * @param bytesParks
	 */
	public void deserializeFrom(BytesPark[] bytesParks){
		numModelCleared += 1;
		if (numModelCleared > MODEL_CLEAR_INTERVAL){
			model.clear();
			tmp.clear();
		}
		for(BytesPark bytesPark: bytesParks){
			for(int offset = 0 ; offset < bytesPark.arraySize(); offset += 8){
				int id = bytesPark.extractIdEach8Bytes(offset);
				float value = bytesPark.extractValueEach8Bytes(offset);
				model.put(id, value);
				tmp.put(id, value);
			}
		}

	}
	
	public float getById(int id){
		return tmp.get(id);
	}
	
	/**
	 * set new value, not just delta
	 * @param id
	 * @param value
	 * @return
	 */
	public void setValue(int id, float value){
		tmp.put(id, value);
	}
	
}
