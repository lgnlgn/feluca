package org.shanbo.feluca.classification.lr;

import java.util.Arrays;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public final class SGDL1LR extends SGDL2LR{

	private double[] qWeights = null;
	private double u = 0.0;
	
	protected void init(){
		qWeights = new double[maxFeatureId + 1];
		Arrays.fill(qWeights, initWeight);
		super.init();
	}
	
	public final double gradientDescend(Vector sample){
		double wTx = w0;
		int innerLabel = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0];
		double newlabel = transform( innerLabel); //{-1, +1}
		for(int i = 0 ; i < sample.getSize(); i++){//wTx
			wTx += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double gradient = - newlabel * ( 1 - 1/(1 + Math.pow(Math.E, - newlabel * wTx)));

		w0 -= alpha  * (gradient + 2 * lambda * w0);
		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[sample.getFId(i)] -= 
					  alpha * (gradient * sample.getWeight(i) + 2 * lambda * featureWeights[sample.getFId(i)]) ;
			applyPenalty(sample.getFId(i));
		}
		double innerPrediction =  1/ (1+Math.pow(Math.E,  - wTx));
		return innerPrediction;

	}
	
	private void applyPenalty(int fid){
		double z = featureWeights[fid]; 
		//w[i]
		if (featureWeights[fid] > 0){
			featureWeights[fid] = Math.max(0, featureWeights[fid] - (u + qWeights[fid]));
		}else if (featureWeights[fid] < 0){
			featureWeights[fid] = Math.min(0, featureWeights[fid] + (u - qWeights[fid]));
		}
		qWeights[fid] = qWeights[fid] + (featureWeights[fid] - z);
	}
	
	
	protected void estimateParameter() throws NullPointerException{
		this.samples = Utilities.getIntFromProperties(dataEntry.getDataStatistic(), DataStatistic.NUM_VECTORS);
		double rate = Math.log(2 + samples /((1 + biasWeightRound)/(biasWeightRound * 2.0)) /( this.maxFeatureId + 0.0));
		if (rate < 0.5)
			rate = 0.5;

		if (alpha == null){
			alpha = 0.5 / rate;
			minAlpha = alpha  / Math.pow(1 + rate, 1.8);
		}
		if (this.lambda == null){
			lambda = 0.5 / rate;
//			minLambda = lambda  / Math.pow(1 + rate, 1.8);
			minLambda = 0.1;
		}
	}
	
	
	@Override
	public int estimate(Properties dataStatus, Properties parameters) {
		// TODO Auto-generated method stub
		int maxFeatureId = Utilities.getIntFromProperties(dataStatus, DataStatistic.MAX_FEATURE_ID);
		int maxVectorSize = Utilities.getIntFromProperties(dataStatus, DataStatistic.MAX_VECTORSIZE);
		int numberLines = Utilities.getIntFromProperties(dataStatus, DataStatistic.NUM_VECTORS);
		int numberFeatures = Utilities.getIntFromProperties(dataStatus, DataStatistic.TOTAL_FEATURES);
		
		int modelSize = maxFeatureId * 4 / 1024 ;
		modelSize += maxFeatureId * 4 / 1024 ;
		int dataSetKb = 0;
		if (parameters.containsKey("inRam")){
			// use file data 
			dataSetKb += 30 * 1024; // VectorStorage.FileStorage approximately cost
//			dataSetKb += VectorPool.RAMEstimate( maxVectorSize);
		}else{
//			dataSetKb += VectorStorage.RAMCompactStorage.RAMEstimate(numberLines, numberFeatures, vectorStatusPara);
		}
		return dataSetKb + modelSize;
	}



}
