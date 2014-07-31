package org.shanbo.feluca.classification.common;

import java.util.Properties;

import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.data2.Vector;




public interface Classifier {

	public void loadData(DataEntry data) throws Exception;
	
	public void train() throws Exception;
		
	public void setProperties(Properties prop);
	
	public Properties getProperties();
	
	public void predict(DataEntry test, String resultPath, Evaluator... evaluators) throws Exception;
	
	/**
	 * predict probabilities of different classes
	 * @param sample
	 * @param probabilities
	 * @throws Exception
	 */
	public void predict(Vector sample, double[] probabilities) throws Exception;
	
	public void saveModel(String filePath) throws Exception;
	
	public void loadModel(String modelPath, Properties statistic) throws Exception;
	
	public void crossValidation(int fold, Evaluator... evaluators) throws Exception;
}
