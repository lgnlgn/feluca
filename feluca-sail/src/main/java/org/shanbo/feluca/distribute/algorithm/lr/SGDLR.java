package org.shanbo.feluca.distribute.algorithm.lr;

import gnu.trove.set.hash.TIntHashSet;

import java.util.List;
import java.util.Map.Entry;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.data.util.CollectionUtil;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.launch.LoopingBase;
import org.shanbo.feluca.distribute.newmodel.PartialVectorModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/**
 * logistic regression
 * @author lgn
 *
 */
public abstract class SGDLR  extends LoopingBase{
	
	final static int LABELRANGEBASE = 32768;
	public final static double DEFAULT_STOP = 0.001;
	public final static int DEFAULT_LOOPS = 30;
		
	
	protected int[][] dataInfo = null;
	
	
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
	
	public SGDLR(GlobalConfig conf) throws Exception {
		super(conf);
		initParams();
		estimateParameter();
	}

	static Logger log = LoggerFactory.getLogger(SGDLR.class);
	static String VECTOR_MODEL_NAME = "sgdlr";
	PartialVectorModel vectorModel;
	
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
	
	@Override
	protected void modelStart() throws Exception {
		modelClient.createVector(VECTOR_MODEL_NAME, conf.getDataStatistic().getIntValue(DataStatistic.MAX_FEATURE_ID), 0f );
	}

	@Override
	protected void modelClose() throws Exception {
		modelClient.dumpVector(VECTOR_MODEL_NAME, Constants.Base.getWorkerRepository() + "/model/" + conf.getAlgorithmName());
	}

	protected void computeLoopBegin() throws Exception {
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
	

	
	


	protected void computeLoopEnd() {
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
