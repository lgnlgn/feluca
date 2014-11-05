package org.shanbo.feluca.classification.lr;

import java.io.IOException;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public class SGDL2LR extends AbstractSGDLogisticRegression{

	private double gradientDescend(Vector sample){
		double weightSum = 0;

		for(int i = 0 ; i < sample.getSize(); i++){
			weightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0] - (1/ (1+tmp)); 
		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[sample.getFId(i)] += 
					alpha * (error * sample.getWeight(i) * partialDerivation - lambda * featureWeights[sample.getFId(i)]) ;
		}
		return error;
	}



	@Override
	protected void _train(int fold, int remain) throws Exception {
		double avge = 99999.9;
		double lastAVGE = Double.MAX_VALUE;

		double corrects  = 0;
		double lastCorrects = -1;


		double multi = (biasWeightRound * minSamples + maxSamples)/(minSamples + maxSamples + 0.0);

		for(int l = 0 ; l < Math.max(10, loops)
						&& (l < Math.min(10, loops) 
						|| (l < loops && (Math.abs(1- avge/ lastAVGE) > convergence )
						|| Math.abs(1- corrects/ lastCorrects) > convergence * 0.01)); l++){
			lastAVGE = avge;
			lastCorrects = corrects;
			dataEntry.reOpen(); //start reading data

			long timeStart = System.currentTimeMillis();

			int c =1; //for n-fold cv
			double error = 0;
			double sume = 0;
			corrects = 0;
			int cc = 0;

			for(Vector sample = dataEntry.getNextVector(); sample != null ; sample = dataEntry.getNextVector()){
				if (c % fold == remain){ // no train
					;
				}else{ //train
					if ( sample.getIntHeader() == this.biasLabel){ //bias; sequentially compute #(bias - 1) times
						for(int bw = 1 ; bw < this.biasWeightRound; bw++){ //bias
							this.gradientDescend(sample);
						}
					}
					error = gradientDescend(sample);
					if (Math.abs(error) < 0.45)//accuracy
						if ( sample.getIntHeader() == this.biasLabel)
							corrects += this.biasWeightRound;
						else
							corrects += 1; 
					cc += 1;
					sume += Math.abs(error);
				}
				c += 1;
			}

			avge = sume / cc;

			long timeEnd = System.currentTimeMillis();
			double acc = corrects / (cc * multi) * 100;

			if (corrects  < lastCorrects ){ //
				if (!alphaSetted){
					this.alpha *= 0.5;
					if (alpha < minAlpha)
						alpha = minAlpha;
				}
				if (!lambdaSetted){
					this.lambda *= 0.9;
					if (lambda < minLambda)
						lambda = minLambda;
				}
			}
			System.out.println(String.format("#%d loop%d\ttime:%d(ms)\tacc: %.3f(approx)\tavg_error:%.6f", cc, l, (timeEnd - timeStart), acc , avge));
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
