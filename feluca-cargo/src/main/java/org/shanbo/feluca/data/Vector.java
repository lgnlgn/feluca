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
 * general vector format. Vector needs only initialize once
 * @author lgn
 *
 */
public abstract class Vector {
	
	public enum VectorType{
		LABEL_FID_WEIGHT,
		FIDONLY,
		VID_FID_WEIGHT,
	}
	
	public final static int LENGTH_PER_EXPANTION = 128 * 1024;
	
	int[] ids ;
	int idSize;
	VectorType inputType;
	VectorType outputType; //
	
	private Vector(){
		ids = new int[64 * 1024];
	}
	
	/**
	 * object serialize to bytes
	 * @param buffer
	 * @return
	 */
	public abstract boolean appendToByteBuffer(ByteBuffer buffer);
	
	/**
	 * should be synchronized. raw text to object
	 * @param line
	 * @return
	 */
	public abstract boolean parseLine(String line);
	
	/**
	 * exclude offset length . e.g. (end - start)
	 * deserialization from bytes
	 * @param cache
	 * @param start
	 * @param end
	 */
	public abstract void set(byte[] cache, int start, int end);
	
	public abstract String toString();
	
	/**
	 * you have to manually expand capacity of the arrays.  
	 */
	abstract void checkAndExpand();
	
	/**
	 * control the output(serialize) format 
	 * @param outputType
	 */
	public void setOutputType(VectorType outputType){
		this.outputType = outputType;
	}
	
	public VectorType getVectorType(){
		return this.outputType;
	}
	
	public int getSize(){
		return idSize;
	}
	
	public long getLongHeader(){
		return 0;
	}
	
	public int getIntHeader(){
		return 0;
	}
	
	public byte[] getHeader(){
		return null;
	}
	
	
	public int getFId(int idx){
		if (idx < 0 || idx >= idSize){
			throw new IndexOutOfBoundsException("vector index out of bound");
		}
		return ids[idx];
	}

	
	public float getWeight(int idx){
		return 0;
	}
	
	public int getIntPayload(int idx){
		return 0;
	}
	
	public long getLongPayload(int idx){
		return 0l;
	}
	
	public float getFloatPayload(int idx) {
		return 0.0f;
	}
	
	public double getDoublePayload(int idx) {
		return 0.0;
	}
	
	

	
	
	public static class FIDVector extends Vector{
		
		private FIDVector(){
			super();
			this.inputType = VectorType.FIDONLY;
			this.outputType = VectorType.FIDONLY;
		}
		
		@Override
		public void set(byte[] cache, int start, int end) {
			idSize = (end- start)/4;
			for(int i = 0 ; i < idSize; ++i){
				int id = BytesUtil.getInt(cache, start + (i << 2));
				ids[i] = id;
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
		public synchronized boolean parseLine(String line) {
			String[] iids = line.split("\\s+");
			idSize = iids.length;
			for(int i =0; i < idSize; i++){
				ids[i] = Integer.parseInt(iids[i]); 
			}
			return true;
		}
		
		final void checkAndExpand(){
			if (idSize >= ids.length){
				int[] tmp = new int[idSize + LENGTH_PER_EXPANTION];
				System.arraycopy(ids, 0, tmp, 0, idSize);
				ids = tmp;
			}
		}
	}
	
	public static class LWVector extends Vector{
		int label;
		float[] weights;
		
		private LWVector(){
			super();
			weights = new float[ids.length];
			this.inputType = VectorType.LABEL_FID_WEIGHT;
			this.outputType = VectorType.LABEL_FID_WEIGHT;
		}
		
		@Override
		public boolean appendToByteBuffer(ByteBuffer buffer) {
			int capacityNeeds = 4 + 4 + (idSize  << 3) ; //(veclenght) + (label) + (kv pairs) each kv-pair occupy 8 bytes
			if (buffer.capacity() - buffer.position() > capacityNeeds){
				buffer.putInt(capacityNeeds - 4);
				buffer.putInt(label);
				for(int i = 0 ; i < idSize; i++){
					buffer.putInt(ids[i]);
					buffer.putFloat(weights[i]);
				}
				return true;
			}else{
				return false;
			}
		}

		@Override
		public float getWeight(int idx){
			return weights[idx];
		}
		
		/**
		 * represent classifier label
		 */
		public int getIntHeader(){
			return label;
		}
		
		@Override
		public void set(byte[] cache, int start, int end) {
			label = BytesUtil.getInt(cache, start);
			idSize  = (end - start - 4)/8; // include the 4-bytes header 
			int fStart = start + 4; // label
			for(int i = 0 ; i < idSize; i++){
				int id = BytesUtil.getInt(cache, fStart + ( i << 3 ));
				float weight = BytesUtil.getFloat(cache, fStart + 4 + (i <<3 ));
				ids[i] = id;
				weights[i] = weight;
				checkAndExpand();
			}
		}
		
		final void checkAndExpand(){
			if (idSize >= ids.length){
				int[] tmp = new int[idSize + LENGTH_PER_EXPANTION];
				System.arraycopy(ids, 0, tmp, 0, idSize);
				ids = tmp;
				float[] tmp2 = new float[idSize + LENGTH_PER_EXPANTION];
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
			Map<String, String> split = Splitter.onPattern("\\s+").withKeyValueSeparator(":").split(labelIds[1].trim());
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
