package org.shanbo.feluca.distribute.model;

import org.shanbo.feluca.common.FelucaException;

public class HashPartitioner{
	int maxShards = 1;
	public HashPartitioner(int maxShards){
		if (maxShards < 0)
			throw new FelucaException("partitions must =0 in HashPartitioner");
		this.maxShards = maxShards;
	}
	
	public int decideShard(int fid) {
		if (maxShards == 1){
			return 0;
		}else if (maxShards == 2){
			return fid & 0x1;
		}else if (maxShards == 4){
			return fid & 0x3;
		}
		return fid % maxShards;
	}

	public int getMaxShards(){
		return maxShards;
	}
	
	/**
	 * from 0 or 1, 
	 */
	public int featureIdToIndex(int fid, int shardId) {
		if (maxShards == 1){
			return fid;
		}else if (maxShards == 2){
			return fid >>> 1;
		}else if (maxShards == 4){
			return fid >>> 2;
		}
		return fid / maxShards;
	}

	public int indexToFeatureId(int index, int shardId) {
		if (maxShards == 1){
			return index ; //shardId == 0
		}else if (maxShards == 2){
			return (index << 1) + shardId;
		}else if (maxShards == 4){
			return (index << 2) + shardId;
		}
		return index * maxShards + shardId;
	}
}