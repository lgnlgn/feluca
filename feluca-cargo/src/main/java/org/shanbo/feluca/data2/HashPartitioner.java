package org.shanbo.feluca.data2;


public class HashPartitioner{
	int maxShards = 1;
	public HashPartitioner(int maxShards){
		if (maxShards < 0)
			throw new RuntimeException("partitions must =0 in HashPartitioner");
		this.maxShards = maxShards;
	}
	
	public int decideShard(int id) {
		if (maxShards == 1){
			return 0;
		}else if (maxShards == 2){
			return id & 0x1;
		}else if (maxShards == 4){
			return id & 0x3;
		}else if (maxShards == 8){
			return id & 0x7;
		}
		return id % maxShards;
	}

	public int getMaxShards(){
		return maxShards;
	}
	
	/**
	 * from 0 or 1, 
	 */
	public int idToIndex(int id, int shardId) {
		if (maxShards == 1){
			return id;
		}else if (maxShards == 2){
			return id >>> 1;
		}else if (maxShards == 4){
			return id >>> 2;
		}else if (maxShards == 8){
			return id >>> 3;
		}
		return id / maxShards;
	}

	public int indexToId(int index, int shardId) {
		if (maxShards == 1){
			return index ; //shardId == 0
		}else if (maxShards == 2){
			return (index << 1) + shardId;
		}else if (maxShards == 4){
			return (index << 2) + shardId;
		}else if (maxShards == 8){
			return (index << 3) + shardId;
		}
		return index * maxShards + shardId;
	}
}