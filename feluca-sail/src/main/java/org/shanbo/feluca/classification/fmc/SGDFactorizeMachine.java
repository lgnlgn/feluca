package org.shanbo.feluca.classification.fmc;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.classification.lr.AbstractSGDLogisticRegression;
import org.shanbo.feluca.classification.lr.SGDL2LR;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.data2.Vector;

/**
 * only support 2-degree interaction of features.
 * Extending AbstractSGDLogisticRegression 
 * 
 * @author lgn
 *
 */
public class SGDFactorizeMachine extends SGDL2LR{

	protected int dim = 5;
	protected float fWeightRange = 0.5f;
	protected float[][] factors ;

	
	Random rand = new Random();

	protected void init(){
		super.init(); 
		factors = new float[dim][];
		for(int k = 0 ; k < dim ; k ++){
			factors[k] = new float[maxFeatureId + 1];
			for(int i = 0 ; i < factors[k].length ; i++){
				factors[k][i] = rand.nextFloat() * fWeightRange;
			}
		}
	}


	public final double gradientDescend(Vector sample){
		double oneDegreeWeightSum = 0;
		for(int i = 0 ; i < sample.getSize(); i++){
			oneDegreeWeightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		
		double intersectionWeightSum = 0;
		double[] Sigmav2x2 = new double[dim];
		double[] SigmaVX = new double[dim];
		for(int f = 0; f < dim ; f ++){
			for(int i = 0 ; i < sample.getSize(); i++){
				SigmaVX[f] = factors[f][i] * featureWeights[sample.getFId(i)];
				Sigmav2x2[f] = Math.pow(factors[f][i], 2) * Math.pow(featureWeights[sample.getFId(i)], 2);
			}
			intersectionWeightSum += ((Math.pow(SigmaVX[f], 2)  - Sigmav2x2[f]));
		}
		double tmp = Math.pow(Math.E, -(oneDegreeWeightSum + (intersectionWeightSum * 0.5))); //e^-sigma(x)
		double error = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0] - (1/ (1+tmp)); 
		
		//-----------w & v[]--------,
		double partialDerivation =   tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < sample.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[sample.getFId(i)] += 
					alpha * (error * sample.getWeight(i) * partialDerivation - lambda * featureWeights[sample.getFId(i)]) ;
			
			for(int f = 0 ; f < dim ; f++){
				double step = (SigmaVX[f] * sample.getWeight(i) - factors[f][i] * sample.getWeight(i) * sample.getWeight(i)) * 2;
				factors[f][i] += alpha * (error * step * partialDerivation - lambda * factors[f][i] ) ;
			}
		}
		//----------v[]-------
		return error;
	}
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		this.dim = new Integer(prop.getProperty("dim", "5"));
	}

	@Override
	public Properties getProperties() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void predict(Vector sample, double[] probabilities) throws Exception {
		double oneDegreeWeightSum = 0;
		for(int i = 0 ; i < sample.getSize(); i++){
			oneDegreeWeightSum += featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		
		double intersectionWeightSum = 0;
		double[] Sigmav2x2 = new double[dim];
		double[] SigmaVX = new double[dim];
		for(int f = 0; f < dim ; f ++){
			for(int i = 0 ; i < sample.getSize(); i++){
				SigmaVX[f] = factors[f][i] * featureWeights[sample.getFId(i)];
				Sigmav2x2[f] = Math.pow(factors[f][i], 2) * Math.pow(featureWeights[sample.getFId(i)], 2);
			}
			intersectionWeightSum += ((Math.pow(SigmaVX[f], 2)  - Sigmav2x2[f]));
		}

		double probability = 1/(1+Math.pow(Math.E, -(oneDegreeWeightSum + (intersectionWeightSum * 0.5))));
		probabilities[0] =  1- probability;
		probabilities[1] =  probability;
		
	}

	@Override
	public void saveModel(String filePath) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void loadModel(String modelPath, Properties statistic)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void crossValidation(int fold, Evaluator... evaluators)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void estimateParameter() throws NullPointerException {
		// TODO Auto-generated method stub
		
	}


	@Override
	public int estimate(Properties dataStatus, Properties parameters) {
		// TODO Auto-generated method stub
		return 0;
	}

}
