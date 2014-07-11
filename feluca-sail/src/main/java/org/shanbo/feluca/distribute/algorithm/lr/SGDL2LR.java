package org.shanbo.feluca.distribute.algorithm.lr;

import java.net.UnknownHostException;
import java.util.Map.Entry;

import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.distribute.launch.GlobalConfig;

import com.alibaba.fastjson.JSONObject;

public class SGDL2LR extends AlgorithmBase{
	
	final static double initWeight = 0;
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
	
	protected Integer loops = DEFAULT_LOOPS;
	protected int fold = 5;
	
	protected int samples = 0; 
	protected int maxFeatureId = -1;
	 
	protected int biasLabel = 0; //  original label
	protected int biasWeightRound = 1;
	
	// for accuracy stop
	protected int minSamples = 0;  // #
	protected int maxSamples = 0;  // #

	double lastCorrects = -1;
	double error = 0;
	double sume = 0.0, 	corrects = 0;
	int cc = 0;
	
	public SGDL2LR(GlobalConfig conf) throws UnknownHostException {
		super(conf);
		initParams();
		estimateParameter();
	}

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
	
	final public void compute(Vector v){
		int label = v.getIntHeader();
		if ( label == this.biasLabel){ //bias; sequentially compute #(bias - 1) times
			for(int bw = 1 ; bw < this.biasWeightRound; bw++){ //bias

				this.gradientDescend(v);
			}
		}

		error = gradientDescend(v);
		if (Math.abs(error) < 0.5)//accuracy
			if ( label == this.biasLabel)
				corrects += this.biasWeightRound;
			else
				corrects += 1; 
		cc += 1;
		sume += Math.abs(error);

	}
	
	private double gradientDescend(Vector v){
		int label = v.getIntHeader();
		double weightSum = 0;

		for(int i = 0 ; i < v.getSize(); i++){
			weightSum += modelClient.getById(v.getFId(i)) * v.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = dataInfo[LABELRANGEBASE + label][0] - (1/ (1+tmp)); 
		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < v.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w)
			float lastWeight = modelClient.getById(v.getFId(i));
			modelClient.setValue(v.getFId(i), 
					(float)(lastWeight + alpha * (error * v.getWeight(i) * partialDerivation - lambda * lastWeight)));

		}
		return error;
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
			lambda = 0.2 / rate;
//			minLambda = lambda  / Math.pow(1 + rate, 1.8);
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
		
		for(Entry<String, Object> entry : algoConf.entrySet()){
			String  key= entry.getKey();
			if (key.startsWith("-w")){
				biasLabel = Integer.parseInt(key.substring(2));
				biasWeightRound = Integer.parseInt(entry.getValue().toString());
			}
		}
	}

	@Override
	protected void checkStopCondition() {
		//TODO
	}
	
	
}
