package org.shanbo.feluca.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.data.util.BytesUtil;

import com.google.common.base.Splitter;

/**
 * general vector format
 * @author lgn
 *
 */
public abstract class Vector {
	
	public enum VectorType{
		LABEL_FID_WEIGHT,
		FIDONLY,
		VID_FID_WEIGHT,
	}
	
	int[] ids ;
	int idSize;
	VectorType type;
	VectorType outputType;
	
	protected Vector(){
		ids = new int[64 * 1024];
	}
	
	/**
	 * 
	 * @param buffer
	 * @return
	 */
	public abstract boolean appendToByteBuffer(ByteBuffer buffer);
	
	/**
	 * should be synchronized
	 * @param line
	 * @return
	 */
	public abstract boolean parseLine(String line);
	
	/**
	 * exclude offset length . e.g. (end - start)
	 * @param cache
	 * @param start
	 * @param end
	 */
	public abstract void set(byte[] cache, int start, int end);
	
	/**
	 * control the output(serialize) format 
	 * @param outputType
	 */
	public void setOutputType(VectorType outputType){
		this.outputType = outputType;
	}
	
	public int getSize(){
		return idSize;
	}
	
	protected long getLongHeader(){
		return 0;
	}
	
	protected int getIntHeader(){
		return 0;
	}
	
	protected byte[] getHeader(){
		return null;
	}
	
	
	public int getFId(int idx){
		if (idx < 0 || idx >= idSize){
			throw new IndexOutOfBoundsException("vector index out of bound");
		}
		return ids[idx];
	}

	
	protected float getWeight(int idx){
		return 0;
	}
	
	protected int getIntPayload(int idx){
		return 0;
	}
	
	protected long getLongPayload(int idx){
		return 0l;
	}
	
	protected float getFloatPayload(int idx) {
		return 0.0f;
	}
	
	protected double getDoublePayload(int idx) {
		return 0.0;
	}
	
	
	protected void checkAndExpand(){
		if (idSize >= ids.length){
			int[] tmp = new int[idSize + 128 * 1024];
			System.arraycopy(ids, 0, tmp, 0, idSize);
			ids = tmp;
		}
	}
	
	
	public static class FIDVector extends Vector{
		
		protected FIDVector(){
			this.type = VectorType.FIDONLY;
			this.outputType = VectorType.FIDONLY;
		}
		
		@Override
		public void set(byte[] cache, int start, int end) {
			int size = (end- start)/4;
			for(int i = 0 ; i < size; i+= 4){
				int id = BytesUtil.getInt(cache, start + i);
				ids[i >> 2] = id;
				idSize += 1;
				checkAndExpand();
			}
		}

		@Override
		public boolean appendToByteBuffer(ByteBuffer buffer) {
			int capacityNeeds = (idSize * 4) + 4;
			if (buffer.capacity() - buffer.arrayOffset() > capacityNeeds){
				buffer.putInt(idSize  * 4);
				for(int i = 0 ; i < idSize; i++){
					buffer.putInt(ids[i]);
				}
				return true;
			}else{
				return false;
			}
		}
		
		public String toString(){
			List<Integer> fids = new ArrayList<Integer>(idSize);
			for(int i = 0 ; i < idSize; i++){
				fids.add(ids[i]);
			}
			return StringUtils.join(fids, " ");
		}

		@Override
		public boolean parseLine(String line) {
			String[] iids = line.split("\\s+");
			idSize = iids.length;
			for(int i =0; i < idSize; i++){
				ids[i] = Integer.parseInt(iids[i]); 
			}
			return true;
		}
		
	}
	
	public static class LWVector extends Vector{
		int label;
		float[] weights;
		
		@Override
		public boolean appendToByteBuffer(ByteBuffer buffer) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public float getWeight(int idx){
			return weights[idx];
		}
		
		public int getIntHeader(){
			return label;
		}
		
		@Override
		public void set(byte[] cache, int start, int end) {
			
			label = BytesUtil.getInt(cache, start);
			int size = (end- start - 4)/8;
			for(int i = 0 ; i < size; i+= 8){
				int id = BytesUtil.getInt(cache, start + i);
				float weight = BytesUtil.getFloat(cache, start + i + 4);
				ids[i >> 3] = id;
				weights[i>>3] = weight;
				idSize += 1;
				checkAndExpand();
			}
		}
		
		final protected void checkAndExpand(){
			if (idSize >= ids.length){
				int[] tmp = new int[idSize + 128 * 1024];
				System.arraycopy(ids, 0, tmp, 0, idSize);
				ids = tmp;
				float[] tmp2 = new float[idSize + 128*1024];
				System.arraycopy(weights, 0, tmp2, 0, idSize);
				weights = tmp2;
			}
		}
		
		public String toString(){
			List<String> tmp = new ArrayList<String>(idSize);
			for(int i = 0 ; i < idSize; i++){
				tmp.add(String.format("%d:%.4f", ids[i], weights[i] ));
			}
			return label + " " + StringUtils.join(tmp, " ");
		}

		@Override
		public boolean parseLine(String line) {
			String[] labelIds = line.split("\\s+", 2);
			this.label = Integer.parseInt(labelIds[0]);
			Map<String, String> split = Splitter.onPattern("\\s+").withKeyValueSeparator(":").split(labelIds[1]);
			int i = 0;
			for(Entry<String, String> kv : split.entrySet()){
				ids[i] = Integer.parseInt(kv.getKey());
				weights[i] = Float.parseFloat(kv.getValue());
				i+=1;
			}
			idSize = i;
			return true;
		}
		
	}
	
	
	/**
	 * decide the analysis way (both serialized and raw_text) for the vector.
	 * @param type
	 * @return
	 */
	public static Vector build(VectorType type){
		if (type == VectorType.FIDONLY){
			return new FIDVector();
		}else{
			return new LWVector();
		}
	}
	
}
