package org.shanbo.feluca.data;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.nio.ByteBuffer;

import org.msgpack.annotation.Message;
import org.shanbo.feluca.data.util.BytesUtil;
import org.shanbo.feluca.data.util.NumericTokenizer;

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

	TIntArrayList ids;
	int idSize;
	VectorType inputType;
	VectorType outputType; //

	
	private Vector(){
	}

	
	/**
	 * object serialize to bytes
	 * @param buffer
	 * @return
	 */
	public abstract int appendToByteBuffer(ByteBuffer buffer);

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
		return ids.getQuick(idx);
	}


	public float getWeight(int idx){
		return 0;
	}

	public byte[] getBytesPayload(int idx){
		return new byte[]{};
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




	/**
	 * fid fid fid...
	 * @author lgn
	 *
	 */
	public static class FIDVector extends Vector{

		private FIDVector(){
			super();
			this.inputType = VectorType.FIDONLY;
			this.outputType = VectorType.FIDONLY;
		}

		@Override
		public void set(byte[] cache, int start, int end) {
			idSize = (end- start)/4;
			if (ids == null){
				ids = new TIntArrayList(idSize);
			}else{
				ids.resetQuick();
			}
			for(int i = 0 ; i < idSize; ++i){
				int id = BytesUtil.getInt(cache, start + (i << 2));
				ids.add(id);
			}
		}

		@Override
		public int appendToByteBuffer(ByteBuffer buffer) {
			int capacityNeeds = (idSize * 4) + 4;
			if (buffer.capacity() - buffer.arrayOffset() > capacityNeeds){
				buffer.putInt(idSize  * 4);
				for(int i = 0 ; i < idSize; i++){
					buffer.putInt(ids.getQuick(i));
				}
				return capacityNeeds;
			}else{
				return -1;
			}
		}

		public String toString(){
			return ids.toString();
		}

		@Override
		public synchronized boolean parseLine(String line) {
			String[] iids = line.split("\\s+");
			idSize = iids.length;
			if (ids == null){
				ids = new TIntArrayList(idSize);
			}else{
				ids.resetQuick();
			}
			for(int i =0; i < idSize; i++){
				ids.add(Integer.parseInt(iids[i]));
			}
			return true;
		}

	}

	/**
	 * lable:[fid:weight],[fid:weight]...
	 * @author lgn
	 *
	 */
	@Message
	public static class LWVector extends Vector{
		int label;
		TFloatArrayList weights;

		private LWVector(){
			super();
			weights = new TFloatArrayList();
//			weights = new float[ids.length];
			this.inputType = VectorType.LABEL_FID_WEIGHT;
			this.outputType = VectorType.LABEL_FID_WEIGHT;
		}

		@Override
		public int appendToByteBuffer(ByteBuffer buffer) {
			int capacityNeeds = 4 + 4 + (idSize  << 3) ; //(veclenght) + (label) + (kv pairs) each kv-pair occupy 8 bytes
			if (buffer.capacity() - buffer.position() > capacityNeeds){
				buffer.putInt(capacityNeeds - 4);
				buffer.putInt(label);
				for(int i = 0 ; i < idSize; i++){
					buffer.putInt(ids.getQuick(i));
					buffer.putFloat(weights.getQuick(i));
				}
				return capacityNeeds;
			}else{
				return -1;
			}
		}

		@Override
		public float getWeight(int idx){
			return weights.getQuick(idx);
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
			if (ids == null){
				ids = new TIntArrayList(idSize);
				weights = new TFloatArrayList(idSize);
			}else{
				ids.resetQuick();
				weights.resetQuick();
			}
			for(int i = 0 ; i < idSize; i++){
				int id = BytesUtil.getInt(cache, fStart + ( i << 3 ));
				float weight = BytesUtil.getFloat(cache, fStart + 4 + (i <<3 ));
				ids.add(id);
				weights.add(weight);
			}
		}


		public String toString(){
			StringBuilder sb = new StringBuilder(label + "");
			for(int i = 0 ; i < idSize; i++){
				sb.append(String.format(" %d:%.4f", ids.getQuick(i), weights.getQuick(i) ));
			}
			return sb.toString();
		}

		@Override
		public boolean parseLine(String line) {
			if (ids == null){ //don't know 
				ids = new TIntArrayList(1024);
				weights = new TFloatArrayList(1024);
			}else{
				ids.resetQuick();
				weights.resetQuick();
			}
			idSize = 0;
			NumericTokenizer nt = new NumericTokenizer();
			nt.load(line);
			this.label = (Integer)(nt.nextNumber());
			while(nt.hasNext()){
				long kv = nt.nextKeyValuePair();
				int fid = NumericTokenizer.extractFeatureId(kv);
				float weight = NumericTokenizer.extractWeight(kv);
				ids.add(fid);
				weights.add(weight);
				idSize += 1;
			}
			return true;
		}

	}

	/**
	 * vid:(fid:weight),(fid,weight)...
	 * same as LWVector
	 * @author lgn
	 *
	 */
	public static class VFWVector extends LWVector{
	}
	
	
	/**
	 * decide the analysis way (both serialized and raw_text) for the vector.
	 * @param type
	 * @return
	 */
	
	public static Vector build(VectorType type){
		if (type == VectorType.FIDONLY){
			return new FIDVector();
		}else if (type == VectorType.VID_FID_WEIGHT){
			return new VFWVector();
		}else {
			return new LWVector();
		}
	}

}
