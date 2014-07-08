package org.shanbo.feluca.distribute.newmodel;

/**
 * ShardDecider is used in client ; while FIdIndexConverter is used in server
 * <p><b>To make them share with the same strategy, use the same parameter: maxShards</b></p>
 * @author lgn
 *
 */
public class HashPartitioner{
	
	public static ShardDecider createShardDecider(int maxShards){
		return new ShardDecider(maxShards);
	}
	
	/**
	 * shardId from 0 to (maxShards-1)
	 * @param maxShards
	 * @param shardId
	 * @return
	 */
	public static FIdIndexConverter createFidIdxConverter(int maxShards, int shardId){
		return new FIdIndexConverter(maxShards, shardId);
	}
	
	/**
	 * client side use
	 * @author lgn
	 *
	 */
	public static class ShardDecider{
		
		private int numShards ;
		
		private ShardDecider(int maxShards){
			this.numShards = maxShards;
		}
		
		public int fidToShardId(int fid){
			return fid % numShards;
		}
	}
	
	/**
	 * <p>This Class is only used in server side ;
	 * <p>but we put it here because it shares the same strategy of {@link ShardDecider}
	 * @author lgn
	 *
	 */
	public static class FIdIndexConverter{
		private int maxShards ;
		private int shardId ;
		
		
		/**
		 * shard id from 0 ~ (maxShards-1)
		 * @param maxShards
		 * @param shardId
		 */
		private FIdIndexConverter(int maxShards, int shardId){
			this.maxShards = maxShards;
			this.shardId = shardId;
		}
		
		/**
		 * <p>for server side compact array;
		 * <p>global featureId map to a compact array on different shards;
		 * <p>e.g. [1,2,3,4,5,6,7,8] -> [1(1),2(3),3(5),4(7)] & [1(2),2(4),3(6),4(8)]
		 *  @param fid  feature id from a Vector
		 *  @return index of the compact array
		 */
		public int featureIdToIndex(int fid) {
			if (maxShards == 1){
				return fid;
			}else if (maxShards == 2) {
				return fid >> 1;
			}else if (maxShards == 4){
				return fid >> 2;
			}else{
				return fid / maxShards;
			}
		}

		/**
		 * use for dump model
		 * @param index
		 * @return
		 */
		public int indexToFeatureId(int index) {
			return index * maxShards + shardId;
		}
		
		public int getMaxShards() {
			return maxShards;
		}
		public int getShardId() {
			return shardId;
		}
		
		
		
	}
}