package org.shanbo.feluca.distribute.classification.lr;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.distribute.launch.LoopingBase;

import org.shanbo.feluca.paddle.GlobalConfig;
import org.shanbo.feluca.util.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class SGDL2LR extends LoopingBase{
	final static int LABELRANGEBASE = 32768;
	public final static double DEFAULT_STOP = 0.001;
	public final static int DEFAULT_LOOPS = 30;
		
	final static int BATCH_COMPUTE_SIZE = 100;
	protected int[][] dataInfo = null;
	
	float[] featureWeights = null; //ref of float[] in ModelLocal
	protected Double alpha = null; // learning speed
	protected Double lambda = null;// regularization
	protected Double convergence = null;
	protected boolean alphaSetted = false;
	protected boolean lambdaSetted = false;

	
	protected double minAlpha = 0.001;
	protected double minLambda = 0.01;
	
	protected int fold = Integer.MAX_VALUE;
	
	protected int samples = 0; 
	protected int maxFeatureId = -1;
	 
	protected int biasLabel = 0; //  original label
	protected int biasWeightRound = 1;
	
	// for accuracy stop
	protected int minSamples = 0;  // #
	protected int maxSamples = 0;  // #

	double lastCorrects = -1;
	double avge = 999999999;
	double lastAVGE = avge;
	double error = 0;
	double sume = 0.0, 	corrects = 0;
	int cc = 0;
	int vcount = 0;  //for cross validation
	int remain = 0; // for cross validation
	long tStart, tEnd;
	double multi ;
	
	public SGDL2LR(GlobalConfig conf) throws Exception {
		super(conf);
		initParams();
		estimateParameter();
	}

	static Logger log = LoggerFactory.getLogger(SGDL2LR.class);

	
	private void initParams(){
		String infoString = conf.getDataStatistic().getString(DataStatistic.LABEL_INFO);
		String[] ll = infoString.split("\\s+");
		String[] classInfo1 = ll[0].split(":"); // orginal_label:converted_label:#num
		String[] classInfo2 = ll[1].split(":");
		
		int[] classInfo1Ints = new int[]{Integer.parseInt(classInfo1[0]), Integer.parseInt(classInfo1[1]), Integer.parseInt(classInfo1[2])};
		int[] classInfo2Ints = new int[]{Integer.parseInt(classInfo2[0]), Integer.parseInt(classInfo2[1]), Integer.parseInt(classInfo2[2])};
		
		this.dataInfo = new int[LABELRANGEBASE * 2][]; // original_LABEL -> index, #sample
		this.dataInfo[LABELRANGEBASE + classInfo1Ints[0]] = new int[]{classInfo1Ints[1], classInfo1Ints[2]};
		this.dataInfo[LABELRANGEBASE + classInfo2Ints[0]] = new int[]{classInfo2Ints[1], classInfo2Ints[2]};

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
		
		this.setProperties(conf.getAlgorithmConf());
		
	}
	
	protected void estimateParameter() throws NullPointerException{
		this.samples = conf.getAlgorithmConf().getIntValue(DataStatistic.NUM_VECTORS);
		double rate = Math.log(2 + samples /((1 + biasWeightRound)/(biasWeightRound * 2.0)) /( this.maxFeatureId + 0.0));
		if (rate < 0.5)
			rate = 0.5;

		if (alpha == null){
			alpha = 0.5 / rate;
			minAlpha = alpha  / Math.pow(1 + rate, 1.8);
		}
		if (this.lambda == null){
			lambda = 0.002 / rate;
			minLambda = 0.01;
		}
	}
	
	private void setProperties(JSONObject algoConf) {
		if (algoConf.containsKey("loops")){
			loops = algoConf.getInteger("loops");
		}
		if (algoConf.containsKey("alpha")){
			alpha = algoConf.getDouble("alpha");
			alphaSetted = true;
		}
		if (algoConf.containsKey("lambda")){
			this.lambda  = algoConf.getDouble("lambda");
			lambdaSetted = true;
		}
		if (algoConf.containsKey("convergence")){
			convergence = algoConf.getDouble("convergence");
		}
		if (this.convergence == null){
			convergence = DEFAULT_STOP;
		}
		for(Entry<String, Object> entry : algoConf.entrySet()){
			String  key= entry.getKey();
			if (key.startsWith("-w")){
				biasLabel = Integer.parseInt(key.substring(2));
				biasWeightRound = Integer.parseInt(entry.getValue().toString());
			}
		}
	}

	protected void startup() {
		modelClient.createVector("weights", JSONUtil.getConf(conf.getDataStatistic(), DataStatistic.MAX_FEATURE_ID, 1) + 1, 0, 0);
		featureWeights = modelClient.getVector("weights");
	}

	
	protected void computeLoopBegin(){
		lastAVGE = avge;
		lastCorrects = corrects;
		tStart = System.currentTimeMillis();
		vcount =1; //for n-fold cv
		error = 0;
		sume = 0;
		corrects = 0;
		cc = 0;
		vcount = 0;
		multi = (biasWeightRound * minSamples + maxSamples)/(minSamples + maxSamples + 0.0);

	}
	
	protected void computeLoopEnd(){
		avge = sume / cc;
		tEnd = System.currentTimeMillis();
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
		System.out.println(String.format("#%d loop%d\ttime:%d(ms)\tacc: %.3f(approx)\tavg_error:%.6f", cc, looping, (tEnd - tStart), acc , avge));
	}
	
	@Override
	protected void computeLoop() throws Exception {
		computeLoopBegin();
		
		ArrayList<Vector> batchVectors = new ArrayList<Vector>();
		for(Vector v = dataEntry.getNextVector(); v!= null; v=dataEntry.getNextVector()){
			batchVectors.add(v);
			if (batchVectors.size() >= BATCH_COMPUTE_SIZE){
				doCompute(batchVectors);
				batchVectors.clear();
			}
		}
		if (batchVectors.size() > 0){
			doCompute(batchVectors);
		}
		computeLoopEnd();
	}

	protected void doCompute(ArrayList<Vector> batchVectors) throws InterruptedException, ExecutionException{
		float[] weightSums = new float[batchVectors.size()];
		for(int i = 0 ; i < batchVectors.size(); i++){
			Vector vector = batchVectors.get(i);
			float weightSum = 0;
			for(int f = 0 ; f < vector.getSize(); f++){
				weightSum += vector.getWeight(f) * featureWeights[vector.getFId(f)];
			}
			weightSums[i] = weightSum;
		}
		//--------
		float[] merged = reducerClient.sum(weightSums);
		for(int i = 0 ; i < batchVectors.size(); i++){
			Vector v = batchVectors.get(i);
			error = gradientDescend(v, ((Float)merged[i]).floatValue());
			if (Math.abs(error) < 0.49)//accuracy
				if ( v.getIntHeader() == this.biasLabel)
					corrects += this.biasWeightRound;
				else
					corrects += 1; 
			cc += 1;
			sume += Math.abs(error);
		}
	}
	
	private double gradientDescend(Vector v, float weightSum){
		int label = v.getIntHeader();
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = dataInfo[LABELRANGEBASE + label][0] - (1/ (1+tmp)); 
		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < v.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w) 
			featureWeights[v.getFId(i)] += 
					alpha * (error * v.getWeight(i) * partialDerivation - lambda * featureWeights[v.getFId(i)]) ;
		}
		return error;
	}
	@Override
	public boolean earlyStop() {
		double errorRatio = 1- avge/ lastAVGE;
		double accRatio = 1- corrects/ lastCorrects;
		System.out.print(String.format("errorRatio:[%.4f] accRatio:[%.4f]  ", errorRatio, accRatio));
		if (  (Math.abs(errorRatio) > convergence )	|| Math.abs(accRatio) > convergence * 0.01){
			
			return false;
		}else{
			return true;
		}
	}
}
