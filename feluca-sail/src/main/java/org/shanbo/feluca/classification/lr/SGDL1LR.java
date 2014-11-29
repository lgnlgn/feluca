package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public final class SGDL1LR extends AbstractSGDLogisticRegression{

	private double[] qWeights = null;
	private double u = 0.0;
	
	protected void init(){
		qWeights = new double[maxFeatureId + 1];
		Arrays.fill(qWeights, initWeight);
		super.init();
	}
	
	public final double gradientDescend(Vector sample){
		double weightSum = 0;
		
		for(int i = 0 ; i < sample.getSize(); i++){
			weightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
//		double error = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0] - (1/ (1+tmp)); //error , (predict_label - correct_label), which is a part of partialDerivation!
//		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;
		double prediction =  1/ (1+tmp);
		int innerLabel = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0];
		double error;
		double partialDerivation =  (tmp)  / (tmp * tmp + 2 * tmp + 1) ;
		//considered moving direction beforehand 
		if (innerLabel == 1){
			error = - (Math.log(prediction) / 0.69314718);
			
		}else{ //0
			error = Math.log(1 - prediction) /0.69314718;
		}
		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation) 
			featureWeights[sample.getFId(i)] += 
					alpha * (error * sample.getWeight(i) * partialDerivation); 
			// apply penalty to [i]th feature
			applyPenalty(sample.getFId(i));
		}
		return error;
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
