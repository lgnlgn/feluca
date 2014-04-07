package org.shanbo.feluca.data;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.shanbo.feluca.data.Vector.VectorType;


public abstract class DataStatistic {

	//basic
	public final static String NUM_VECTORS = "numVectors";
	public final static String MAX_FEATURE_ID = "maxFeatureId";
	public final static String TOTAL_FEATURES = "totalFeatures";
	public final static String MAX_VECTORSIZE = "maxVectorSize";

	
	//with weight
	public final static String SUM_WEIGHTS = "sumWeights";
	public final static String LABEL_INFO = "labelInfo";
	public final static String CLASSES =  "classes";
	//with id
	public final static String MAX_VECTOR_ID = "maxVectorId";

	VectorType statVectorType;
	public DataStatistic counter;

	protected DataStatistic(DataStatistic counter){
		if (counter != null){
			this.counter = counter;
		}
	}

	protected abstract void doStat(Vector vector);
	protected abstract Properties getStatResult();
	protected abstract void clear();
	
	public final void stat(Vector vector){
		if (counter != null){
			counter.doStat(vector);
		}
		doStat(vector);
		this.statVectorType = vector.outputType;
	}
	
	public final void clearStat(){
		if (counter != null){
			counter.clear();
		}
		clear();
	}
	

	public String toString(){
		Properties p = new Properties();
		if (counter != null){
			p.putAll( counter.getStatResult());
		}
		p.putAll(getStatResult());
		p.put("vectorType", statVectorType);
		StringBuilder builder = new StringBuilder();
		for(Entry<Object, Object> entry : p.entrySet()){
			builder.append(entry.getKey() + "=" + entry.getValue() + "\n");
		}
		
		return builder.toString();
	}

	public static class BasicStatistic extends DataStatistic{
		int numVectors = 0;
		int totalFeatures = 0;
		int maxFeatureId = 0;
		int maxVectorSize = 1;
		
		public BasicStatistic() {
			super(null);
			clear();
		}

		@Override
		protected void doStat(Vector vector) {
			numVectors += 1;
			totalFeatures += vector.getSize();
			maxVectorSize = vector.getSize() > maxVectorSize ? vector.getSize(): maxVectorSize;
			for(int i = 0 ; i < vector.getSize(); i++){
				maxFeatureId = vector.getFId(i) > maxFeatureId ?vector.getFId(i):maxFeatureId ;
			}
		}

		@Override
		protected Properties getStatResult() {
			Properties p = new Properties();
			p.put(NUM_VECTORS, this.numVectors);
			p.put(MAX_FEATURE_ID, this.maxFeatureId);
			p.put(TOTAL_FEATURES, this.totalFeatures);
			p.put(MAX_VECTORSIZE, this.maxVectorSize);
			return p;
		}

		@Override
		protected void clear() {
			numVectors = 0;
			totalFeatures = 0;
			maxFeatureId = 0;
		}

	}

	/**
	 * just for test
	 * @author lgn
	 *
	 */
	public static class MinStatistic extends DataStatistic{

		int minId = Integer.MAX_VALUE;
		public MinStatistic(DataStatistic counter) {
			super(counter);
			clear();
		}

		@Override
		protected void doStat(Vector vector) {
			for(int i = 0 ; i < vector.getSize(); i++){
				minId = vector.getFId(i) < minId ?vector.getFId(i):minId ;
			}
			
		}

		@Override
		protected Properties getStatResult() {
			Properties p = new Properties();
			p.put("minFeatureId", this.minId);
			return p;
		}

		@Override
		protected void clear() {
			minId = Integer.MAX_VALUE;
		}
	}
	
	public static class LableWeightStatistic extends DataStatistic{

		double weightSum ;
		HashMap<Integer, int[]> labelInfoBag = new HashMap<Integer, int[]>();
		int i = 0;
		
		public LableWeightStatistic(DataStatistic counter) {
			super(counter);
			
		}
		@Override
		protected void doStat(org.shanbo.feluca.data.Vector vector) {
			int[] labelInfo = labelInfoBag.get(vector.getIntHeader());
			if (labelInfo == null){
				labelInfoBag.put(vector.getIntHeader(), new int[]{labelInfoBag.size(), 1});
			}else{
				labelInfo[1] += 1;
			}
			for(int i = 0 ; i < vector.getSize(); i++){
				weightSum += vector.getWeight(i);
			}
		}
		@Override
		protected Properties getStatResult() {
			Properties p = new Properties();
			p.put(SUM_WEIGHTS, this.weightSum);
			p.put(CLASSES, this.labelInfoBag.size());
			StringBuilder sb = new StringBuilder();
			for(Entry<Integer, int[]> entry : labelInfoBag.entrySet()){
				sb.append(String.format("%d:%d:%d ", entry.getKey(), entry.getValue()[0], entry.getValue()[1]));
			}
			p.put(LABEL_INFO, sb.toString());
			return p;
		}
		@Override
		protected void clear() {
			labelInfoBag.clear();
			weightSum = 0.0;
		}
	}
	
}
