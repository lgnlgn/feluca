package org.shanbo.feluca.data;

import java.util.Map.Entry;
import java.util.Properties;

import org.shanbo.feluca.data.Vector.VectorType;


public abstract class DataStatistic {

	//basic
	public final static String NUM_VECTORS = "numVectors";
	public final static String MAX_FEATURE_ID = "maxFeatureId";
	public final static String TOTAL_FEATURES = "totalFeatures";

	//with weight
	public final static String SUM_WEIGHTS = "sumWeights";

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

	
	public final void stat(Vector vector){
		if (counter != null){
			counter.doStat(vector);
		}
		doStat(vector);
		this.statVectorType = vector.outputType;
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

		public BasicStatistic() {
			super(null);
		}

		@Override
		protected void doStat(Vector vector) {
			numVectors += 1;
			totalFeatures += vector.getSize();
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
			return p;
		}

	}

	public static class MinStatistic extends DataStatistic{

		int minId = Integer.MAX_VALUE;
		protected MinStatistic(DataStatistic counter) {
			super(counter);
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

		
	}
	
}
