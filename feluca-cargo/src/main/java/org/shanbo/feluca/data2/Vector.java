package org.shanbo.feluca.data2;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.text.StrBuilder;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data2.util.NumericTokenizer;
import org.shanbo.feluca.data2.util.NumericTokenizer.FeatureWeight;

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
	
	protected abstract boolean readObject(Object... values);
	
	public abstract String toString();
	
	public abstract List<Vector> divideByFeature(HashPartitioner partitioner);
	
	public abstract void swallow(Vector v);
	
	public abstract int getSpaceCost();
	
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
	
	public static Vector create(VectorType vt, Object... values) throws IOException{
		Vector v = create(vt);
		v.readObject(values);
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
//				long kv = nt.nextKeyValuePair();
				FeatureWeight nextKeyWeight = nt.nextKeyWeight();
//				int fid = nextKeyWeight.getId();
//				float weight = NumericTokenizer.extractWeight(kv);
				fids.add(nextKeyWeight.getId());
				weights.add(nextKeyWeight.getWeight());
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

		@Override
		public int getSpaceCost() {
			return 4 + (fids.size() << 3 ) ; //label + id[] * 4 + weight[] * 4 
		}

		@Override
		protected boolean readObject(Object... values) {
			label = (Integer)values[0];
			fids = (TIntArrayList)values[1];
			weights = (TFloatArrayList)values[2];
			return true;
		}

		@Override
		public List<Vector> divideByFeature(HashPartitioner partitioner) {
			List<Vector> vectors = new ArrayList<Vector>(partitioner.getMaxShards());
			List<StrBuilder> lines = new ArrayList<StrBuilder>(partitioner.getMaxShards());
			for(int i = 0 ; i < partitioner.getMaxShards(); i++){
				vectors.add(create(getOutVectorType()));
				lines.add(new StrBuilder().append(getIntHeader())); //label 
			}
			for(int i = 0 ; i < getSize(); i++){
				int shardId = partitioner.decideShard(getFId(i));
				lines.get(shardId).append(String.format(" %d:%.4f", fids.getQuick(i), weights.getQuick(i) ));
			}
			for(int i = 0; i < vectors.size(); i++){
				vectors.get(i).parseLine(lines.get(i).toString());
			}
			return vectors;
		}

		@Override
		public void swallow(Vector v) {
			if (v == null)
				return;
			if (fids == null){
				fids = new TIntArrayList();
				weights = new TFloatArrayList();
				label = v.getIntHeader();
			}
			for(int i = 0 ; i < v.getSize(); i++){
				fids.add(v.getFId(i));
				weights.add(v.getWeight(i));
			}
		}
	}
	
	public static class VIDVector extends LWVector{
		
	}
	
}
