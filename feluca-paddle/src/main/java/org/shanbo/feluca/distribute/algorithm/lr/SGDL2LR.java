package org.shanbo.feluca.distribute.algorithm.lr;

import gnu.trove.set.hash.TIntHashSet;

import java.util.List;

import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.util.CollectionUtil;
import org.shanbo.feluca.distribute.launch.GlobalConfig;

public class SGDL2LR extends SGDLR{

	public SGDL2LR(GlobalConfig conf) throws Exception {
		super(conf);
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
			
			//compute each vector
			for(long offset : segment){ 
				Vector v = dataReader.getVectorByOffset(offset);
				if (vcount % fold == remain){ // no train
					;
				}else{ //train
					if ( v.getIntHeader() == this.biasLabel){ //bias; sequentially compute #(bias - 1) times
						for(int bw = 1 ; bw < this.biasWeightRound; bw++){ //bias
							this.gradientDescend(v);
						}
					}
					error = gradientDescend(v);
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
			System.out.print("!");
		}
	}
	
	private double gradientDescend(Vector v){
		int label = v.getIntHeader();
		double weightSum = 0;

		for(int i = 0 ; i < v.getSize(); i++){
			weightSum += vectorModel.get(v.getFId(i)) * v.getWeight(i);
		}
		double tmp = Math.pow(Math.E, -weightSum); //e^-sigma(x)
		double error = dataInfo[LABELRANGEBASE + label][0] - (1/ (1+tmp)); 
		double partialDerivation =  tmp  / (tmp * tmp + 2 * tmp + 1) ;

		for(int i = 0 ; i < v.getSize(); i++){
			// w <- w + alpha * (error * partial_derivation - lambda * w)
			float lastWeight = vectorModel.get(v.getFId(i));
			vectorModel.set(v.getFId(i), 
					(float)(lastWeight + alpha * (error * v.getWeight(i) * partialDerivation - lambda * lastWeight)));

		}
		return error;
	}
}
