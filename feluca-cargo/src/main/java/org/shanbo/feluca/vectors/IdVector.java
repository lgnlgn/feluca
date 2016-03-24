package org.shanbo.feluca.vectors;

import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.data2.DataSetInfo.Statistic;

/**
 * Same structure as LabelWeightVector</p>
 * Can be used for collaborative filtering type; graph type
 * @author lgn
 *
 */
public class IdVector extends LabelVector{
	
	public static class MaxIdStatistic extends Statistic{
		int maxVId = Integer.MIN_VALUE;

		@Override
		public void statAsOne() {
			maxVId = maxVId > ((IdVector)current).getVectorId() ? maxVId : ((IdVector)current).getVectorId();
		}

		@Override
		public void statOnFeature(int index) {
			
		}

		@Override
		public Properties getStatInfo() {
			Properties p = new Properties();
			p.put(MAX_VECTOR_ID, this.maxVId);
			return p;
		}
		
	}
	
	
	public static class WeightStatistic extends Statistic{
		double weightSum ;
		@Override
		public void statAsOne() {
		}

		@Override
		public void statOnFeature(int index) {
			weightSum += ((GeneralVector)current).getWeight(index);
		}

		@Override
		public Properties getStatInfo() {
			Properties p = new Properties();
			p.put(SUM_WEIGHTS, this.weightSum);
			return p;
		}
		
	}
	
	public IdVector(){
		this.inputType = VectorType.VID_FID_WEIGHT;
		this.outputType = VectorType.VID_FID_WEIGHT;
	}
	
	public int getVectorId(){
		return getLabel();
	}
	
	@Override
	public List<Statistic> getStat() {
		List<Statistic> stat = super.getStat();
		stat.add(new MaxIdStatistic());
		return stat;
	}
}