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

/**
 * minimize least square loss
 * @author lgn
 *
 */
public abstract class AbstractSGDLogisticRegression implements Classifier, MemoryEstimater{
	protected final static double initWeight = 0;
	protected final static int LABELRANGEBASE = 32768;
	public final static double DEFAULT_STOP = 0.001;
	public final static int DEFAULT_LOOPS = 30;


	protected double w0;
	protected int w0Type = 0;// 0 for no use; 1 for stay ; 2 for gradient 
	public double[] featureWeights = null;

	protected DataEntry dataEntry= null;

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
	protected int biasWeightRound = -1;

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
		if (biasWeightRound == -1){
			this.biasWeightRound = Math.round(ratio);
		}
		float cc = this.biasWeightRound * this.minSamples + this.maxSamples + 0.0f;
		w0 = - Math.log(cc/(this.biasWeightRound * this.minSamples) -1 );
		if (w0Type == 0 || w0Type == 2)
			w0 = 0;
		System.out.println(w0);
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


	protected double logloss(int y, double yy) {
		if (y == 0){
			return - Math.log( 1- yy) / 0.69314718;
		}else{
			return - Math.log(yy) / 0.69314718;
		}
	}

	protected boolean acc(int y , double yy) {
		if (y == 1 && yy > 0.5){
			return true;
		}else if (y == 0 && yy < 0.5){
			return true;
		}
		return false;
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

		if (prop.getProperty("w0type") != null){
			setW0Type(Utilities.getIntFromProperties(prop,"w0type"));
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
		bw.write(w0 + "\n");
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
		w0 = Double.parseDouble(br.readLine());
		for(String line = br.readLine(); line != null; line = br.readLine()){
			String[] fidWeight = line.split("\t");
			this.featureWeights[Integer.parseInt(fidWeight[0])] = Double.parseDouble(fidWeight[1]);
		}
		br.close();
	}


	public void predict(Vector sample, double[] probabilities) throws Exception{
		double weigtSum = w0 ; 
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
		System.out.println(w0);
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

	public void setW0Type(int type){
		if (type > 2 || type < 0){
			throw new RuntimeException("0 for no use; 1 for stay ; 2 for gradient");
		}
		this.w0Type = type;
	}


	protected void _train(int fold, int remain) throws Exception{
		double avge = 99999.9;
		double lastAVGE = Double.MAX_VALUE;

		double corrects  = 0;
		double lastCorrects = -1;


		double multi = (biasWeightRound * minSamples + maxSamples)/(minSamples + maxSamples + 0.0);

		for(int l = 0 ; l < Math.max(5, loops)
				&& (l < Math.min(5, loops) 
						|| (l < loops && (Math.abs(1- avge/ lastAVGE) > convergence )
								|| Math.abs(1- corrects/ lastCorrects) > convergence * 0.01)); l++){
			lastAVGE = avge;
			lastCorrects = corrects;
			dataEntry.reOpen(); //start reading data

			long timeStart = System.currentTimeMillis();

			int c =1; //for n-fold cv
			double innerPredict = 0; //0~1
			double sume = 0;
			corrects = 0;
			int cc = 0;


			int pp = 10;
			for(Vector sample = dataEntry.getNextVector(); sample != null ; sample = dataEntry.getNextVector()){
				if (c % fold == remain){ // no train
					;
				}else{ //train
					//bias; sequentially compute #(bias - 1) times
					innerPredict = this.gradientDescend(sample);
					if (acc(outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0], innerPredict)){
						corrects += 1;
					}
					cc += 1;
					sume += logloss(outerLabelInfo[LABELRANGEBASE + sample.getIntHeader()][0], innerPredict);
					if ( sample.getIntHeader() == this.biasLabel){
						for(int bw = 0 ; bw < this.biasWeightRound; bw++){ //bias
							innerPredict = this.gradientDescend(sample);
						}
					}

				}
				c += 1;
				if (c% pp == 0){
					System.out.print(String.format("[%.4f:%.1f]", sume / cc, corrects * 100 / cc));
					pp *= 2;
				}
			}

			avge = sume / cc;

			long timeEnd = System.currentTimeMillis();
			double acc = corrects / (cc ) * 100;

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
			System.out.println(w0);
			System.out.println(String.format("#%d loop%d\ttime:%d(ms)\tacc: %.3f(approx)\tavg_error:%.6f", cc, l, (timeEnd - timeStart), acc , avge));
		}
	}

	abstract protected  double  gradientDescend(Vector sample) ;

	public abstract int estimate(Properties dataStatus, Properties parameters) ;
}
