package org.shanbo.feluca.vectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.lang3.text.StrBuilder;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data2.DataSetInfo.Statistic;
import org.shanbo.feluca.data2.HashPartitioner;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Vector.VectorType;
import org.shanbo.feluca.data2.util.BytesUtil;
import org.shanbo.feluca.data2.util.NumericTokenizer;
import org.shanbo.feluca.data2.util.NumericTokenizer.FeatureWeight;

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

public abstract class GeneralVector extends Vector{

	public static class BasicStatistic extends Statistic{

		int numVectors = 0;
		int totalFeatures = 0;
		int maxFeatureId = 0;
		int maxVectorSize = 1;
		
		@Override
		public void statAsOne() {
			numVectors += 1;
			totalFeatures += this.current.getSize();
			maxVectorSize = this.current.getSize() > maxVectorSize ? this.current.getSize(): maxVectorSize;
		}

		@Override
		public void statOnFeature(int index) {
			maxFeatureId =  this.current.getFId(index) > maxFeatureId 
					?this.current.getFId(index):maxFeatureId ;
		}

		@Override
		public Properties getStatInfo() {
			Properties p = new Properties();
			p.put(NUM_VECTORS, this.numVectors);
			p.put(MAX_FEATURE_ID, this.maxFeatureId);
			p.put(TOTAL_FEATURES, this.totalFeatures);
			p.put(MAX_VECTORSIZE, this.maxVectorSize);
			return p;
		}
		
	}

	byte[] head;
	TFloatArrayList weights;
	
	@Override
	public void pack(Packer packer) throws IOException {
		packer.write(head);
		packer.write(this.fids.toArray());
		packer.write(this.weights.toArray());
	}

	@Override
	public void unpack(Unpacker unpacker) throws IOException {
		this.head = unpacker.read(byte[].class);
		this.fids = new TIntArrayList(unpacker.read(int[].class));
		this.weights = new TFloatArrayList(unpacker.read(float[].class));
	}

	protected byte[] getHeader(){
		return head;
	}
	
	
	public float getWeight(int idx){
		return weights.getQuick(idx);
	}
	

	@Override
	public int getSpaceCost() {
		return head.length + (fids.size() << 3 ) ; //label + id[] * 4 + weight[] * 4 
	}

	@Override
	protected boolean readObject(Object... values) {
		head = (byte[])values[0];
		fids = (TIntArrayList)values[1];
		weights = (TFloatArrayList)values[2];
		return true;
	}

}
