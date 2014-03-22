package org.shanbo.feluca.distribute.model;

import org.shanbo.feluca.common.FelucaException;

public interface Partitioner{
	
	/**
	 * client side
	 * @param id
	 * @return
	 */
	public int decidePartition(int id);
	
	/**
	 * 
	 * server side calls. maps sparse ids to a tight ids array
	 * be careful of index starts 
	 * @param id
	 * @return
	 */
	public int decideIndexById(int id);
	
	public static class HashPartitioner implements Partitioner{
		int partitions = 0;
		public HashPartitioner(int partitions){
			if (partitions < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.partitions = partitions;
		}
		
		public int decidePartition(int id) {
			return id % partitions;
		}

		/**
		 * from 0 or 1, 
		 */
		public int decideIndexById(int id) {
			return id / partitions;
		}
	}
	
	/**
	 * TODO not implemented yet
	 * @author lgn
	 *
	 */
	public static class RangePartitioner implements Partitioner{
		int partitions = 0;
		int idMax = 0;
		public RangePartitioner(int idMax, int partitions){
			if (partitions < 0)
				throw new FelucaException("partitions must =0 in HashPartitioner");
			this.partitions = partitions;
		}
		
		public int decidePartition(int id) {
			return id % partitions;
		}

		/**
		 * from 0 or 1, 
		 */
		public int decideIndexById(int id) {
			return id / partitions;
		}
	}
	
}