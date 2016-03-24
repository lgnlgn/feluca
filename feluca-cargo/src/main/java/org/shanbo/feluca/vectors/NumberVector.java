package org.shanbo.feluca.vectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.text.StrBuilder;
import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataSetInfo.Statistic;
import org.shanbo.feluca.data2.util.BytesUtil;
import org.shanbo.feluca.data2.util.NumericTokenizer;
import org.shanbo.feluca.data2.util.NumericTokenizer.FeatureWeight;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;

/**
 * libsvm format except the label is real number <p></p> For regression 
 * @author lgn
 *
 */
public class NumberVector extends GeneralVector{

	/**
	 * used for averaging regression value
	 * @author lgn
	 *
	 */
	public static class VectorValueStatistic extends Statistic{

		double valueSum = 0;
		
		@Override
		public void statAsOne() {
			valueSum += ((NumberVector)current).getVectorNumber();
		}

		@Override
		public void statOnFeature(int index) {
		}

		@Override
		public Properties getStatInfo() {
			Properties p = new Properties();
			p.put(SUM_VEC_VALUE, this.valueSum);
			return p;
		}
	}
	
	public NumberVector(){
		this.inputType = VectorType.NUMBER_FID_WEIGHT;
		this.outputType = VectorType.NUMBER_FID_WEIGHT;
	}
	
	public float getVectorNumber(){
		return BytesUtil.bytes2Float(head);
	}
	
	@Override
	public boolean parseLine(String line) {
		if (fids == null){ //don't know 
			fids = new TIntArrayList(1024);
			weights = new TFloatArrayList(1024);
			head = new byte[4];
		}else{
			fids.resetQuick();
			weights.resetQuick();
		}
		NumericTokenizer nt = new NumericTokenizer();
		nt.load(line);
		BytesUtil.float2Bytes(nt.nextNumber().floatValue(), head);
		while(nt.hasNext()){
			FeatureWeight nextKeyWeight = nt.nextKeyWeight();
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
		StringBuilder sb = new StringBuilder(String.format("%.3f", BytesUtil.bytes2Float(head)));
		for(int i = 0 ; i < fids.size(); i++){
			sb.append(String.format(" %d:%.4f", fids.getQuick(i), weights.getQuick(i) ));
		}
		return sb.toString();
	}

    @Deprecated
	public List<Vector> divideByFeature(HashPartitioner partitioner) {
		List<Vector> vectors = new ArrayList<Vector>(partitioner.getMaxShards());
		List<StrBuilder> lines = new ArrayList<StrBuilder>(partitioner.getMaxShards());
		for(int i = 0 ; i < partitioner.getMaxShards(); i++){
			vectors.add(new LabelVector());
			lines.add(new StrBuilder().append(String.format("%.3f" ,getVectorNumber()))); //label 
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Statistic> getStat() {
		List<Statistic> stats = new ArrayList<Statistic>();
		stats.add(new VectorValueStatistic());
		stats.add(new BasicStatistic());
		return stats;
	}
	
}
