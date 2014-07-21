package org.shanbo.feluca.distribute.newmodel;

import java.util.List;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntFloatHashMap;

/**
 * The methods are only allow to be invoke according to 'fetch-then-update' sequence 
 * @author lgn
 *
 */
public class PartialVectorModel {
	static int TMP_LIST_CAPACITY_INIT = 512;
	
	int MAX_TMP_VECTOR_SIZE = 100000;
	int MAX_TMP_FIDS_PER_SHARD = 10000;
	
	private TIntFloatHashMap vectorInMap; //new
	
	TIntArrayList[] convertedFidsBuffer;
	TFloatArrayList[] toUpdateValuesBuffer;
	
	HashPartitioner partitioner;
	
	
	public PartialVectorModel(HashPartitioner partitioner){
		this(partitioner, 100000, 10000);
	}
	
	public PartialVectorModel(HashPartitioner partitioner, int vectorSizeCompactPoint, int bufferSizeCompactPoint){
		this.MAX_TMP_VECTOR_SIZE = vectorSizeCompactPoint;
		this.MAX_TMP_FIDS_PER_SHARD = bufferSizeCompactPoint;
		this.vectorInMap = new TIntFloatHashMap();
		this.partitioner = partitioner;
		convertedFidsBuffer = new TIntArrayList[partitioner.getMaxShards()];
		toUpdateValuesBuffer = new TFloatArrayList[partitioner.getMaxShards()];
		for(int i = 0 ; i < partitioner.getMaxShards(); i++){
			convertedFidsBuffer[i] = new TIntArrayList(TMP_LIST_CAPACITY_INIT);
			toUpdateValuesBuffer[i] = new TFloatArrayList(TMP_LIST_CAPACITY_INIT);
		}
	}
	
	
	void checkTmpBufferList(){
		for(int i = 0 ; i  < convertedFidsBuffer.length; i++){
			if (convertedFidsBuffer[i].size() > MAX_TMP_FIDS_PER_SHARD){
				convertedFidsBuffer[i].clear(TMP_LIST_CAPACITY_INIT);
				toUpdateValuesBuffer[i].clear(TMP_LIST_CAPACITY_INIT);
			}else{
				convertedFidsBuffer[i].resetQuick();
				toUpdateValuesBuffer[i].resetQuick();
			}
		}
	}
	
	
	void checkVectorAndCompact(){	
		if (vectorInMap.size() >= MAX_TMP_VECTOR_SIZE){
			vectorInMap = new TIntFloatHashMap();
		}
	}
	
	public float get(int fid){
		return vectorInMap.get(fid);
	}
	
	public void set(int fid, float value){
		vectorInMap.put(fid, value);
	}
	
	void mergeValuesList(List<float[]> valuesList){
		this.checkVectorAndCompact();
		for(int shardId = 0 ; shardId < partitioner.getMaxShards(); shardId++){
			TIntArrayList tmpConvertedFids = convertedFidsBuffer[shardId];
			float[] mGetThisShard = valuesList.get(shardId);
			for(int fi = 0 ; fi < tmpConvertedFids.size(); fi++){
				int convertedFid = tmpConvertedFids.getQuick(fi);
				int originalFid = partitioner.indexToFeatureId(convertedFid, shardId);
				float value = mGetThisShard[fi];
				this.set(originalFid, value);
			}
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
	
	public float[][] splitValues(int[] fids){
		for(int i = 0 ; i < toUpdateValuesBuffer.length;i++){
			toUpdateValuesBuffer[i].resetQuick();
		}
		for(int fi = 0 ; fi < fids.length; fi++){
			int shardId = partitioner.decideShard(fids[fi]);
			toUpdateValuesBuffer[shardId].add(vectorInMap.get(fids[fi])); //this order will be the same as in VectorClient
		}
		float[][] result = new float[toUpdateValuesBuffer.length][];
		for(int i = 0; i < toUpdateValuesBuffer.length ; i++ ){
			result[i] = toUpdateValuesBuffer[i].toArray();
		}
		return result;
	}
	
}
