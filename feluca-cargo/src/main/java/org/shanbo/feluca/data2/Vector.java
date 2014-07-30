package org.shanbo.feluca.data2;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.io.IOException;

import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data.util.NumericTokenizer;

public abstract class Vector {
	
	public enum VectorType{
		LABEL_FID_WEIGHT,
		VID_FID_WEIGHT,
	}
	
	TIntArrayList fids;
	VectorType inputType;  //use only for convert or build
	VectorType outputType; //

	
	public abstract void pack(Packer packer) throws IOException;
	
	public abstract void unpack(Unpacker unpacker) throws IOException;
	
	public abstract boolean parseLine(String line);
	
	public abstract String toString();
	
	public VectorType getOutVectorType(){
		return this.outputType;
	}
	public void setOutputType(VectorType outputType){
		this.outputType = outputType;
	}
	
	public int getSize(){
		return fids.size();
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
		return fids.getQuick(idx);
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

	public static Vector create(VectorType vt){
		Vector v;
		if (vt == VectorType.LABEL_FID_WEIGHT){
			v = new LWVector();
		}else {
			v = new VIDVector();
		}
		return v;
	}
	
	public static Vector create(VectorType vt, String line){
		Vector v = create(vt);
		v.parseLine(line);
		return v;
	}
	
	public static Vector create(VectorType vt, Unpacker unpacker) throws IOException{
		Vector v = create(vt);
		v.unpack(unpacker);
		return v;
	}
	
	
	public double getDoublePayload(int idx) {
		return 0.0;
	}
	
	public static class LWVector extends Vector{

		int label ;
		TFloatArrayList weights;
		
		public LWVector(){
			this.inputType = VectorType.LABEL_FID_WEIGHT;
			this.outputType = VectorType.LABEL_FID_WEIGHT;
		}
		
		@Override
		public void pack(Packer packer) throws IOException {
			packer.write(label);
			packer.write(this.fids.toArray());
			packer.write(this.weights.toArray());
		}

		@Override
		public void unpack(Unpacker unpacker) throws IOException {
			this.label = unpacker.readInt();
			this.fids = new TIntArrayList(unpacker.read(int[].class));
			this.weights = new TFloatArrayList(unpacker.read(float[].class));
		}

		public int getIntHeader(){
			return label;
		}
		
		public float getWeight(int idx){
			return weights.getQuick(idx);
		}
		
		@Override
		public boolean parseLine(String line) {
			if (fids == null){ //don't know 
				fids = new TIntArrayList(1024);
				weights = new TFloatArrayList(1024);
			}else{
				fids.resetQuick();
				weights.resetQuick();
			}
			NumericTokenizer nt = new NumericTokenizer();
			nt.load(line);
			this.label = (Integer)(nt.nextNumber());
			while(nt.hasNext()){
				long kv = nt.nextKeyValuePair();
				int fid = NumericTokenizer.extractFeatureId(kv);
				float weight = NumericTokenizer.extractWeight(kv);
				fids.add(fid);
				weights.add(weight);
			}
			if (fids.size() == 0){
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder(label + "");
			for(int i = 0 ; i < fids.size(); i++){
				sb.append(String.format(" %d:%.4f", fids.getQuick(i), weights.getQuick(i) ));
			}
			return sb.toString();
		}
	}
	
	public static class VIDVector extends LWVector{
		
	}
	
}
