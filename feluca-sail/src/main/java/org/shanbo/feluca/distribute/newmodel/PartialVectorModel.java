package org.shanbo.feluca.distribute.newmodel;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.hash.TIntFloatHashMap;

public class PartialVectorModel {
	
	final static int MAX_TMP_VECTOR_SIZE = 100000;
	
	TIntFloatHashMap map; //new
	TIntFloatHashMap toUpdate; //old
	
	public PartialVectorModel(){
		this.map = new TIntFloatHashMap();
		this.toUpdate = new TIntFloatHashMap();
		
	}
	
	
	public void checkAndCompact(){
		if (map.size() >= MAX_TMP_VECTOR_SIZE){
			map.clear();
			toUpdate.clear();
			map.compact();
			toUpdate.compact();
		}
	}
	
	public float get(int fid){
		return map.get(fid);
	}
	
	public void set(int fid, float value){
		map.put(fid, value);
	}
	
	public void setForMerge(int fid, float value){
		map.put(fid, value);
		toUpdate.put(fid, value);
	}
	
	
	void splitValuesByFIds(int[] fids , FidPartitioner partitioner, TFloatArrayList[] splitTo){
		for(int fi = 0 ; fi < fids.length; fi++){
			int shardId = partitioner.decideShard(fids[fi]);
			splitTo[shardId].add(map.get(fids[fi]) - toUpdate.get(fids[fi])); //this order will be the same as in VectorClient
		}
	}
	
}
