package org.shanbo.feluca.classification.lr;

import java.util.Properties;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public class SGDL2LR extends AbstractSGDLogisticRegression{

	/**
	 * 1->1 ; 0-> -1; 0.5->0
	 * @param y
	 * @return
	 */
	protected double transform(double y ){
		return y * 2 - 1 ; 
	}
	
	protected double transform(int y ){
		return (y * 2) - 1 ; 
	}
	
	protected double gradientDescend(Vector sample){
		double weightSum = w0;
		for(int i = 0 ; i < sample.getSize(); i++){
			weightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double prediction =  1/ (1+tmp);
		int innerLabel = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0];
		
		double newlabel = transform( innerLabel);
		double pred = transform(prediction);
		
		double error = newlabel * (1 /(1 + Math.pow(Math.E, newlabel * pred)) - 1);
		double partialDerivation =    (tmp)  / (tmp * tmp + 2 * tmp + 1) ;
//		//considered moving direction beforehand 
//		error = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0] - (1/ (1+tmp)); 
		w0 -= alpha * 2 * (error *  partialDerivation + lambda * w0);
		//be careful! partialDerivation here is missing coefficient x , i.e. sample.getWeight(i);
		//we will add it at the gradient phrase below.
		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[sample.getFId(i)] -= 
					 2 * alpha * (error * sample.getWeight(i) * partialDerivation + lambda * featureWeights[sample.getFId(i)]) ;
		}
		return prediction;
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
