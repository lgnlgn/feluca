package org.shanbo.feluca.distribute.newmodel;

import org.shanbo.feluca.common.FelucaException;

public interface FidPartitioner{
	
	/**
	 * feature_id -> partition_id
	 * @param id
	 * @return
	 */
	public int decideShard(int fid);
	
	
	/**
	 * 
	 * server side calls. maps sparse ids to a tight ids array
	 * be careful of index starts 
	 * @param id
	 * @return
	 */
	public int featureIdToIndex(int fid, int shardId);
	
	/**
	 * 
	 * server side calls. recall index to feature id
	 * be careful of index starts 
	 * @param id
	 * @return
	 */
	public int indexToFeatureId(int index, int shardId);
	
	
	public static class HashPartitioner implements FidPartitioner{
		int maxShards = 1;
		public HashPartitioner(int maxShards){
			if (maxShards < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.maxShards = maxShards;
		}
		
		public int decideShard(int fid) {
			return fid % maxShards;
		}

		/**
		 * from 0 or 1, 
		 */
		public int featureIdToIndex(int fid, int shardId) {
			return fid / maxShards;
		}

		public int indexToFeatureId(int index, int shardId) {
			return index * maxShards + shardId;
		}
	}
	
	/**
	 * not tested yet
	 * @author lgn
	 *
	 */
	@Deprecated
	public static class RangePartitioner implements FidPartitioner{
		int partitions = 0;
		int idsPerPartition = 0;
		int idMax = 0;
		public RangePartitioner(int idMax, int partitions){
			if (partitions < 0 || idMax < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.partitions = partitions;
			idsPerPartition = (idMax + 1) / partitions;
		}
		
		public int decideShard(int fid) {
			return fid / partitions;
		}

		/**
		 * from 0 or 1, 
		 */
		public int featureIdToIndex(int fid,int shardId) {
			return fid - ( idsPerPartition * partitions ) ;
		}

		public int indexToFeatureId(int index, int partition) {
			return index + idsPerPartition * partition;
		}


	}
	
}