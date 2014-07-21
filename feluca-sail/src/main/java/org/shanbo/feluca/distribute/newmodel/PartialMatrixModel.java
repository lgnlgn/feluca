package org.shanbo.feluca.distribute.newmodel;

import java.util.List;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

//TODO
public class PartialMatrixModel {
	static int TMP_LIST_CAPACITY_INIT = 512;
	
	int MAX_TMP_VECTOR_SIZE = 100000;
	int MAX_TMP_FIDS_PER_SHARD = 10000;
	
	TIntObjectHashMap<float[]> matrixInMap ;
	
	TIntArrayList[] convertedFidsBuffer;
	
	HashPartitioner partitioner;
	
	public PartialMatrixModel(HashPartitioner partitioner){
		this(partitioner, 100000, 10000);
	}
	
	public PartialMatrixModel(HashPartitioner partitioner, int vectorSizeCompactPoint, int bufferSizeCompactPoint){
		this.MAX_TMP_VECTOR_SIZE = vectorSizeCompactPoint;
		this.MAX_TMP_FIDS_PER_SHARD = bufferSizeCompactPoint;
		this.matrixInMap = new TIntObjectHashMap<float[]>();
		this.partitioner = partitioner;
		convertedFidsBuffer = new TIntArrayList[partitioner.getMaxShards()];
		for(int i = 0 ; i < partitioner.getMaxShards(); i++){
			convertedFidsBuffer[i] = new TIntArrayList(TMP_LIST_CAPACITY_INIT);
		}
	}
	
	void checkTmpBufferList(){
		for(int i = 0 ; i  < convertedFidsBuffer.length; i++){
			if (convertedFidsBuffer[i].size() > MAX_TMP_FIDS_PER_SHARD){
				convertedFidsBuffer[i].clear(TMP_LIST_CAPACITY_INIT);
			}else{
				convertedFidsBuffer[i].resetQuick();
			}
		}
	}
	void checkMatrixAndCompact(){	
		if (matrixInMap.size() >= MAX_TMP_VECTOR_SIZE){
			matrixInMap = new TIntObjectHashMap<float[]>();
		}
	}
	public int[][] splitFids(int[] fids){
		for(int i = 0 ; i < convertedFidsBuffer.length;i++){
			convertedFidsBuffer[i].resetQuick();
		}
		for(int fid : fids){
			int shardId = partitioner.decideShard(fid);
			convertedFidsBuffer[shardId].add(partitioner.featureIdToIndex(fid, shardId));
		}
		return splitFidsQuick();
	}
	
	/**
	 * return current converted fids[] in the buffer
	 * @return
	 */
	public int[][] splitFidsQuick(){
		int[][] result = new int[convertedFidsBuffer.length][];
		for(int i = 0; i < convertedFidsBuffer.length ; i++ ){
			result[i] = convertedFidsBuffer[i].toArray();
		}
		return result;
	}
	
	
	public float[][][] splitValues(int[] fids){
		float[][][] result = new float[convertedFidsBuffer.length][][]; //[shards][[columns][columns]...[]]
		int[] indexes = new int[convertedFidsBuffer.length]; //
		for(int i = 0; i < convertedFidsBuffer.length ; i++ ){
			result[i] = new float[convertedFidsBuffer[i].size()][];
			indexes[i] = 0;
		}
		for(int fi = 0 ; fi < fids.length; fi++){
			int shardId = partitioner.decideShard(fids[fi]);
			float[][] toShardValues = result[shardId];
			toShardValues[indexes[shardId]++] = matrixInMap.get(fids[fi]);
		}
		return result;
	}
	
	public float[] get(int fid){
		return matrixInMap.get(fid);
	}
	
	public void set(int fid, float[] row){
		matrixInMap.put(fid, row);
	}
	
	public void mergeValuesList(List<float[][]> valuesList){
		this.checkMatrixAndCompact();
		for(int shardId = 0 ; shardId < partitioner.getMaxShards(); shardId++){
			TIntArrayList tmpConvertedFids = convertedFidsBuffer[shardId];
			float[][] mGetThisShard = valuesList.get(shardId);
			for(int fi = 0 ; fi < tmpConvertedFids.size(); fi++){
				int convertedFid = tmpConvertedFids.getQuick(fi);
				int originalFid = partitioner.indexToFeatureId(convertedFid, shardId);
				float[] value = mGetThisShard[fi];
				this.set(originalFid, value);
			}
		}
	}
	
}
