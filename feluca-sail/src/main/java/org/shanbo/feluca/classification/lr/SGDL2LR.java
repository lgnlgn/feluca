package org.shanbo.feluca.classification.lr;

import java.util.Properties;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public class SGDL2LR extends AbstractSGDLogisticRegression{

	protected double gradientDescend(Vector sample){
		double weightSum = 0;
		for(int i = 0 ; i < sample.getSize(); i++){
			weightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0] - (1/ (1+tmp)); 
		double partialDerivation =  (tmp)  / (tmp * tmp + 2 * tmp + 1) ;
		//be careful! partialDerivation here is missing coefficient x , i.e. sample.getWeight(i);
		//we will add it at the gradient phrase below.
		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[sample.getFId(i)] += 
					alpha * (error * sample.getWeight(i) * partialDerivation - lambda * featureWeights[sample.getFId(i)]) ;
		}
		return error;
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



	@Override
	protected void estimateParameter(){

		this.samples = Utilities.getIntFromProperties(dataEntry.getDataStatistic(), DataStatistic.NUM_VECTORS);
		double rate = Math.log(2 + samples /((1 + biasWeightRound)/(biasWeightRound * 2.0)) /( this.maxFeatureId + 0.0));
		if (rate < 0.5)
			rate = 0.5;

		if (alpha == null){
			alpha = 0.5 / rate;
			minAlpha = alpha  / Math.pow(1 + rate, 1.8);
			System.out.println("guessing alpha:" + alpha);
		}
		if (this.lambda == null){
			lambda = 0.02 / rate;
			//			minLambda = lambda  / Math.pow(1 + rate, 1.8);
			minLambda = 0.001;
			System.out.println("guessing lambda:" + lambda);
		}
	}

}
