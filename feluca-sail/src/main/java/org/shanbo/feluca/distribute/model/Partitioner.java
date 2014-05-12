package org.shanbo.feluca.distribute.model;

import org.shanbo.feluca.common.FelucaException;

public interface Partitioner{
	
	/**
	 * feature_id -> partition_id
	 * @param id
	 * @return
	 */
	public int decidePartition(int fid);
	
	/**
	 * 
	 * server side calls. maps sparse ids to a tight ids array
	 * be careful of index starts 
	 * @param id
	 * @return
	 */
	public int featureIdToIndex(int fid);
	
	/**
	 * 
	 * server side calls. recall index to feature id
	 * be careful of index starts 
	 * @param id
	 * @return
	 */
	public int indexToFeatureId(int index, int partition);
	
	
	public static class HashPartitioner implements Partitioner{
		int partitions = 1;
		public HashPartitioner(int partitions){
			if (partitions < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.partitions = partitions;
		}
		
		public int decidePartition(int fid) {
			return fid % partitions;
		}

		/**
		 * from 0 or 1, 
		 */
		public int featureIdToIndex(int fid) {
			return fid / partitions;
		}

		public int indexToFeatureId(int index, int partition) {
			return index * partitions + partition;
		}
	}
	
	/**
	 *
	 * @author lgn
	 *
	 */
	public static class RangePartitioner implements Partitioner{
		int partitions = 0;
		int idsPerPartition = 0;
		int idMax = 0;
		public RangePartitioner(int idMax, int partitions){
			if (partitions < 0 || idMax < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.partitions = partitions;
			idsPerPartition = (idMax + 1) / partitions;
		}
		
		public int decidePartition(int fid) {
			return fid / partitions;
		}

		/**
		 * from 0 or 1, 
		 */
		public int featureIdToIndex(int fid) {
			return fid - ( idsPerPartition * partitions ) ;
		}

		public int indexToFeatureId(int index, int partition) {
			return index + idsPerPartition * partition;
		}


	}
	
}