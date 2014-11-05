package org.shanbo.feluca.classification.lr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Map.Entry;



import org.shanbo.feluca.classification.common.Classifier;
import org.shanbo.feluca.classification.common.Evaluator;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.MemoryEstimater;
import org.shanbo.feluca.paddle.common.Utilities;

public abstract class AbstractSGDLogisticRegression implements Classifier, MemoryEstimater{
	final static double initWeight = 0;
	final static int LABELRANGEBASE = 32768;
	public final static double DEFAULT_STOP = 0.001;
	public final static int DEFAULT_LOOPS = 30;

	public double[] featureWeights = null;

	protected DataEntry dataEntry= null;

	boolean usePool = false;

	protected int[][] outerLabelInfo = null; //outer label -> info
	protected int[][] innerLabelInfo = null; //inner label -> info

	protected Double alpha = null; // learning speed
	protected Double lambda = null;// regularization
	protected Double convergence = null;
	protected boolean alphaSetted = false;
	protected boolean lambdaSetted = false;


	protected double minAlpha = 0.001;
	protected double minLambda = 0.01;

	protected Integer loops = DEFAULT_LOOPS;
	protected int fold = 5;

	protected int samples = 0; 
	protected int maxFeatureId = -1;

	protected int biasLabel = 0; //  original label
	protected int biasWeightRound = 1;

	// for accuracy stop
	protected int minSamples = 0;  // #
	protected int maxSamples = 0;  // #

	public void loadData(DataEntry data) throws Exception {
		dataEntry = data;
		if (this.dataEntry == null){
			throw new RuntimeException("dataEntry must be set!");
		}else{
			maxFeatureId = Utilities.getIntFromProperties(dataEntry.getDataStatistic(), DataStatistic.MAX_FEATURE_ID);

			String tmpInfo = Utilities.getStrFromProperties(dataEntry.getDataStatistic(), DataStatistic.LABEL_INFO);
			this.outerLabelInfo = new int[LABELRANGEBASE * 2][];
			if (tmpInfo.split(" ").length > 2){
				throw new RuntimeException("Data Set contains more than 2 classes");
			}
			_loadDataInfo(tmpInfo);
		}
	}

	private void _loadDataInfo(String infoString){
		String[] ll = infoString.split("\\s+");
		String[] classInfo1 = ll[0].split(":"); // orginal_label:converted_label:#num
		String[] classInfo2 = ll[1].split(":");

		int[] classInfo1Ints = new int[]{Integer.parseInt(classInfo1[0]), Integer.parseInt(classInfo1[1]), Integer.parseInt(classInfo1[2])};
		int[] classInfo2Ints = new int[]{Integer.parseInt(classInfo2[0]), Integer.parseInt(classInfo2[1]), Integer.parseInt(classInfo2[2])};

		this.outerLabelInfo = new int[LABELRANGEBASE * 2][]; // original_LABEL -> innerLabel, #sample
		this.outerLabelInfo[LABELRANGEBASE + classInfo1Ints[0]] = new int[]{classInfo1Ints[1], classInfo1Ints[2]};
		this.outerLabelInfo[LABELRANGEBASE + classInfo2Ints[0]] = new int[]{classInfo2Ints[1], classInfo2Ints[2]};

		this.innerLabelInfo = new int[2][];  //innerLabel -> original_LABEL, #sample
		this.innerLabelInfo[classInfo1Ints[1]] = new int[]{classInfo1Ints[0], classInfo1Ints[2]};
		this.innerLabelInfo[classInfo2Ints[1]] = new int[]{classInfo2Ints[0], classInfo2Ints[2]};
		
		//  set bias automatically
		float ratio = classInfo2Ints[2] /(classInfo1Ints[2] + 0.0f);
		this.biasLabel = classInfo1Ints[0];
		this.minSamples  = classInfo1Ints[2];
		this.maxSamples  = classInfo2Ints[2];
		if (classInfo1Ints[2] > classInfo2Ints[2]){ // #(label 0) >  #(label 1)
			this.biasLabel = classInfo2Ints[0];
			ratio = classInfo1Ints[2] /(classInfo2Ints[2] + 0.0f);
			this.minSamples  = classInfo2Ints[2];
			this.maxSamples  = classInfo1Ints[2];
		}else{ //default
			;
		}
		this.biasWeightRound = Math.round(ratio);

		try{
			this.estimateParameter();
			if (this.convergence == null){
				convergence = DEFAULT_STOP;
			}
		}catch(NullPointerException e){
			// loading model
			System.out.println( " a model loading~");
		}

	}

	abstract protected void estimateParameter() throws NullPointerException;

	public void train() throws Exception {
		this.init();
		this._train(Integer.MAX_VALUE, 0); // all samples for training

	}

	protected void init(){
		featureWeights = new double[maxFeatureId + 1];
		Arrays.fill(featureWeights, initWeight);
	}


	public void crossValidation(int fold, Evaluator... evaluators) throws Exception {

		for(int i = 0 ; i < fold; i++){
			this.init();
			System.out.println("----cross validation loop " + i);
			this._train(fold, i);
			//-------------test-------

			this.dataEntry.reOpen();
			int c = 1;
			double[] resultProbs = new double[2];

			System.out.println("testing");
			for(Vector sample = dataEntry.getNextVector(); sample != null ; sample = dataEntry.getNextVector()){
				if (c % fold == i){
					if (sample.getSize() == 0)
						continue;
					this.predict(sample, resultProbs);
					for(Evaluator e : evaluators){
						e.collect(outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0], resultProbs);
					}
				}
			}
		}
	}

	public void setProperties(Properties prop) {
		if (prop.getProperty("loops") != null){
			loops = Utilities.getIntFromProperties(prop, "loops");
		}
		if (prop.getProperty("alpha") != null){
			alpha = Utilities.getDoubleFromProperties(prop, "alpha");
			alphaSetted = true;
		}
		if (prop.getProperty("lambda") != null){
			this.lambda = Utilities.getDoubleFromProperties(prop,"lambda");
			lambdaSetted = true;
		}
		if (prop.getProperty("convergence") != null){
			convergence = Utilities.getDoubleFromProperties(prop,"convergence");
		}


		for(Entry<Object, Object> entry : prop.entrySet()){
			String  key= entry.getKey().toString();
			if (key.startsWith("-w")){
				biasLabel = Integer.parseInt(key.substring(2));
				biasWeightRound = Integer.parseInt(entry.getValue().toString());
			}
		}
	}

	public void saveModel(String filePath) throws Exception {		
		BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
		//		bw.write(Utilities.getStrFromProperties(dataEntry.getDataStatistic(), DataStatistic.MAX_FEATURE_ID) + "\n");
		//		bw.write(Utilities.getStrFromProperties(dataEntry.getDataStatistic(), DataStatistic.LABEL_INFO) + "\n");
		for(int i = 0 ; i < this.featureWeights.length; i++){
			if (this.featureWeights[i] != initWeight)
				bw.write(String.format("%d\t%.6f\n", i, this.featureWeights[i]));			
		}
		bw.close();
	}

	public void loadModel(String modelPath, Properties statistic) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(modelPath));	
		this.featureWeights = new double[Integer.parseInt(statistic.getProperty(DataStatistic.MAX_FEATURE_ID)) + 1];
		this._loadDataInfo(statistic.getProperty(DataStatistic.LABEL_INFO));
		for(String line = br.readLine(); line != null; line = br.readLine()){
			String[] fidWeight = line.split("\t");
			this.featureWeights[Integer.parseInt(fidWeight[0])] = Double.parseDouble(fidWeight[1]);
		}
		br.close();
	}


	public void predict(Vector sample, double[] probabilities) throws Exception{
		double weigtSum = 0 ; 
		for(int i = 0 ; i < sample.getSize(); i++){
			weigtSum += this.featureWeights[sample.getFId(i)] * sample.getWeight(i);
		}
		double probability = 1/(1+Math.pow(Math.E, -weigtSum));
		probabilities[0] =  1- probability;
		probabilities[1] =  probability;
	}

	/**
	 * predict probability for data; the predict_Label will accord with training data;
	 * Otherwise use {@link #predict(String, String, Evaluator...)} instead.
	 */
	public void predict(DataEntry data, String resultPath, Evaluator... evaluators) throws Exception {
		if (this.featureWeights == null)
			throw new IOException("!Model haven't been initialized yet! :(");
		BufferedWriter bw = new BufferedWriter(new FileWriter(resultPath));
		bw.write("testLabel\tpredictLabel\tprobability(here means confidence)\n");
		double[] resultProbs = new double[2];
		int innerLabel = -1;
		data.reOpen();
		for(Vector sample = data.getNextVector(); sample != null ; sample = data.getNextVector()){

			if (sample.getSize() == 0){ //how to predict without any features? A default probability = 0.5 should be moderate;
				bw.write(String.format("%d\t%d\t%.4f\n" , sample.getIntHeader(), innerLabelInfo[0][0], 0.5f));
			}
			this.predict(sample, resultProbs);
			if (evaluators != null){
				for(Evaluator e : evaluators){
					int testLabel = outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0]; //innerLabel = [0 or 1]
					e.collect(testLabel, resultProbs);
				}
			}
			innerLabel = resultProbs[0] > resultProbs[1] ? 0 : 1; // predict inner Label with probabilities;
			//output original label;
			bw.write(String.format("%d\t%d\t%.4f\n" , sample.getIntHeader(), innerLabelInfo[innerLabel][0], resultProbs[innerLabel]));
		}
		bw.close();
		for(Evaluator e : evaluators){
			System.out.println(e.resultString());
		}
	}

	public String toString(){
		return String.format("alpha:%.6f, lambda:%.9f, loops: %d, bias:%d on %d times", 
				this.alpha, this.lambda, this.loops, biasLabel, biasWeightRound);
	}


	public Properties getProperties() {
		Properties p = new Properties();
		p.put("alpha", this.alpha);
		p.put("loops", this.loops);
		p.put("lambda", this.lambda);	
		return p;
	}

	protected abstract void _train(int fold, int remain) throws Exception;

	public abstract int estimate(Properties dataStatus, Properties parameters) ;
}
