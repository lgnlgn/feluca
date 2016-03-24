package org.shanbo.feluca.vectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

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
 * label/tag for classification
 * <p></p>
 * libsvm format->  int fid1:weight1 fid2:weight2 fid3:weight3....
 * @author lgn
 *
 */
public class LabelVector extends GeneralVector{
	public static class LabelStatistic extends Statistic{
		HashMap<Integer, int[]> labelInfoBag = new HashMap<Integer, int[]>();

		@Override
		public void statAsOne() {
			LabelVector lv = (LabelVector)this.current;
			int[] labelInfo = labelInfoBag.get(lv.getLabel());
			if (labelInfo == null){
				labelInfoBag.put(lv.getLabel(), new int[]{labelInfoBag.size(), 1});
			}else{
				labelInfo[1] += 1;
			}
		}

		@Override
		public void statOnFeature(int index) {
		}

		@Override
		public Properties getStatInfo() {
			Properties p = new Properties();
			p.put(CLASSES, this.labelInfoBag.size());
			StringBuilder sb = new StringBuilder();
			for(Entry<Integer, int[]> entry : labelInfoBag.entrySet()){
				sb.append(String.format("%d:%d:%d ", entry.getKey(), entry.getValue()[0], entry.getValue()[1]));
			}
			p.put(LABEL_INFO, sb.toString());
			return p;
		}
		
	}
	public LabelVector(){
		this.inputType = VectorType.LABEL_FID_WEIGHT;
		this.outputType = VectorType.LABEL_FID_WEIGHT;
//		this.head = new byte[4];
//		this.fids = new TIntArrayList();
//		this.weights = new TFloatArrayList();
	}
	
	public int getLabel(){
		return BytesUtil.getInt(getHeader());
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
		BytesUtil.int2Byte(nt.nextNumber().intValue(), head);
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
	
	@Deprecated
	public List<Vector> divideByFeature(HashPartitioner partitioner) {
		List<Vector> vectors = new ArrayList<Vector>(partitioner.getMaxShards());
		List<StrBuilder> lines = new ArrayList<StrBuilder>(partitioner.getMaxShards());
		for(int i = 0 ; i < partitioner.getMaxShards(); i++){
			vectors.add(new LabelVector());
			lines.add(new StrBuilder().append(getLabel())); //label 
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
			head = ((GeneralVector)v).getHeader();
		}
		for(int i = 0 ; i < v.getSize(); i++){
			fids.add(v.getFId(i));
			weights.add(((GeneralVector)v).getWeight(i));
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(this.getLabel() + "");
		for(int i = 0 ; i < fids.size(); i++){
			sb.append(String.format(" %d:%.4f", fids.getQuick(i), weights.getQuick(i) ));
		}
		return sb.toString();
	}

	@Override
	public List<Statistic> getStat() {
		List<Statistic> stats = new ArrayList<Statistic>();
		stats.add(new LabelStatistic());
		stats.add(new BasicStatistic());
		return stats;
	}
}