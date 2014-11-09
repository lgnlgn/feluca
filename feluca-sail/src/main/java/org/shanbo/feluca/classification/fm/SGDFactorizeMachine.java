package org.shanbo.feluca.classification.fm;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.classification.lr.AbstractSGDLogisticRegression;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.data2.Vector;

/**
 * only support 2-degree interaction of features.
 * Extending AbstractSGDLogisticRegression 
 * 
 * @author lgn
 *
 */
public class SGDFactorizeMachine extends AbstractSGDLogisticRegression{

	protected int dim = 5;
	
	protected float[][] factors ;
	Random rand = new Random();

	protected void init(){
		super.init();
		factors = new float[dim][];
		for(int i = 0 ; i < dim ; i ++){
			
		}
	}
	
	@Override
	public void train() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setProperties(Properties prop) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Properties getProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void predict(DataEntry test, String resultPath,
			Evaluator... evaluators) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void predict(Vector sample, double[] probabilities) throws Exception {
		// TODO Auto-generated method stub
		
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
	protected void _train(int fold, int remain) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int estimate(Properties dataStatus, Properties parameters) {
		// TODO Auto-generated method stub
		return 0;
	}

}
