package org.shanbo.feluca.distribute.algorithm.lr;

import gnu.trove.set.hash.TIntHashSet;

import java.util.List;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.data.util.CollectionUtil;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.model.PartialVectorModel;


public class SGDL1LR extends SGDLR{

	static String Q_WEIGHT_VECTOR = "qWeight";
	
	PartialVectorModel qWeight ;
	
	double u = 0;
	
	public SGDL1LR(GlobalConfig conf) throws Exception {
		super(conf);
	}
	
	@Override
	protected void modelStart() throws Exception {
		modelClient.createVector(VECTOR_MODEL_NAME, conf.getDataStatistic().getIntValue(DataStatistic.MAX_FEATURE_ID), 0f );
		modelClient.createVector(Q_WEIGHT_VECTOR, conf.getDataStatistic().getIntValue(DataStatistic.MAX_FEATURE_ID), 0f );
		
	}

	protected void computeLoopBegin() throws Exception {
		u = u + alpha * lambda;
		super.computeLoopBegin();
	}
	
	@Override
	protected void computeBlock() throws Exception {
		long[] offsetArray = dataReader.getOffsetArray(); 
		List<long[]> splitted = CollectionUtil.splitLongs(offsetArray, 1000, false);
		//continue split 1000 per block
		for(long[] segment : splitted){
			TIntHashSet idSet = new TIntHashSet();
			// distinct fids
			for(long offset : segment){ 
				Vector v = dataReader.getVectorByOffset(offset);
				distinctIds(idSet, v);
			}
			int[] currentFIds = idSet.toArray();
			//fetch to local
			vectorModel = modelClient.vectorRetrieve(VECTOR_MODEL_NAME, currentFIds);
			qWeight = modelClient.vectorRetrieve(Q_WEIGHT_VECTOR, currentFIds);
			//compute each vector
			for(long offset : segment){ 
				Vector v = dataReader.getVectorByOffset(offset);
				if (vcount % fold == remain){ // no train
					;
				}else{ //train
					if ( v.getIntHeader() == this.biasLabel){ //bias; sequentially compute #(bias - 1) times
						for(int bw = 1 ; bw < this.biasWeightRound; bw++){ //bias
							this.updateWeights(v);
						}
					}
					error = updateWeights(v);
					if (Math.abs(error) < 0.45)//accuracy
						if ( v.getIntHeader() == this.biasLabel)
							corrects += this.biasWeightRound;
						else
							corrects += 1; 
					cc += 1;
					sume += Math.abs(error);
				}
				vcount += 1; 
			}
			modelClient.vectorUpdate(VECTOR_MODEL_NAME, currentFIds);
			modelClient.vectorUpdate(Q_WEIGHT_VECTOR, currentFIds);
			System.out.print("!");
		}
	}
	
	private double updateWeights(Vector v){
		double weightSum = 0;
		
		for(int i = 0 ; i < v.getSize(); i++){
			weightSum += vectorModel.get(v.getFId(i)) * v.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = dataInfo[LABELRANGEBASE + v.getIntHeader()][0] - (1/ (1+tmp)); //error , (predict_label - correct_label), which is a part of partialDerivation!
		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < v.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation) 
			float oldWeight = vectorModel.get(v.getFId(i));
			vectorModel.set(v.getFId(i), (float)(oldWeight + 
					alpha * error * v.getWeight(i) * partialDerivation)); 
			// apply penalty to [i]th feature
			applyPenalty(v.getFId(i));
		}
		return error;
	}
	
	private void applyPenalty(int fid){
		float z = vectorModel.get(fid); 
		float q = qWeight.get(fid);
		//w[i]
		if (z > 0){
			vectorModel.set(fid, (float)Math.max(0, z - (u + q)));
		}else if (z < 0){
			vectorModel.set(fid, (float)Math.min(0, z + (u - q)));
		}
		qWeight.set(fid, q + (vectorModel.get(fid) - z))  ;
	}
	
	/**
	 * different to L2 
	 */
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
			lambda = 0.5 / rate;
			minLambda = 0.01;
		}
	}
}
