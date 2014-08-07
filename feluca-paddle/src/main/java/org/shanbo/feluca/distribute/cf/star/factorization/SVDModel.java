package org.shanbo.feluca.distribute.cf.star.factorization;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import gnu.trove.set.hash.TIntHashSet;

import org.shanbo.feluca.cf.common.RatingInfo;
import org.shanbo.feluca.cf.common.UserRatings;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.distribute.launch.LoopingBase;
import org.shanbo.feluca.paddle.GlobalConfig;
import org.shanbo.feluca.paddle.common.Utilities;
import org.shanbo.feluca.util.JSONUtil;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

public class SVDModel extends LoopingBase{

	final static int BATCH_SIZE = 200;

	protected float[][] userspace = null;
	protected float[][] itemspace = null;

	protected float alpha = 0.003f;
	protected float lambda = 0.001f;
	protected float convergence = 0.97f;
	protected int factor = 50;
	protected int loops = 10;

	protected int maxuid ;
	protected int maxiid ;
	protected float avgrating ;
	protected int totalrating;
	protected int uids ;
	protected int maxRates ; //max number of items rated by a user (i.e. max number of features within a vector)

	protected float vibration;

	float learningSpeed ;
	float totalError;
	int n;
	public SVDModel(GlobalConfig conf) throws Exception {
		super(conf);
		setParam();
		loadStat();
	}

	private void setParam() {
		alpha = JSONUtil.getConf(conf.getAlgorithmConf(), "alpha", 0.005f);
		lambda = JSONUtil.getConf(conf.getAlgorithmConf(), "lambda", 0.003f);
		convergence = JSONUtil.getConf(conf.getAlgorithmConf(), "convergence", 0.95f);
		factor = JSONUtil.getConf(conf.getAlgorithmConf(), "factor", 50);
		learningSpeed = this.alpha;
	}

	private void loadStat(){
		maxuid = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_VECTOR_ID);
		maxiid = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_FEATURE_ID);
		avgrating = (float)(Utilities.getDoubleFromProperties(dataEntry.getDataStatistic(),DataStatistic.SUM_WEIGHTS) 
				/ Utilities.getDoubleFromProperties(dataEntry.getDataStatistic(),DataStatistic.TOTAL_FEATURES));
		totalrating = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.TOTAL_FEATURES);
		uids = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.NUM_VECTORS);
		maxRates = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_VECTORSIZE);
		vibration = (float) Math.sqrt(avgrating / factor);
	}



	protected void startup(){
		modelClient.createMatrix("itemSpace", maxiid + 1, factor, 0, vibration);
		itemspace = modelClient.getMatrix("itemSpace");
		modelClient.createMatrix("userSpace", maxuid + 1, factor, 0, vibration);
		userspace = modelClient.getMatrix("userSpace");
	}


	protected void doCompute(ArrayList<Vector> batchVectors) throws InterruptedException, ExecutionException{
		TIntHashSet userIds = new TIntHashSet();
		for(Vector v : batchVectors){
			UserRatings ur = new UserRatings(v);
			userIds.add(ur.getUid());		
		}
		List<Future<Integer>> matrixUpdate = modelClient.matrixUpdate("userSpace", userIds.toArray()); //sync user
		for(Vector v : batchVectors){
			UserRatings ur = new UserRatings(v);
			for(RatingInfo ri = ur.getNormalNextRating(); ri != null ; ri = ur.getNormalNextRating()){
				float eui = ri.rating - Utilities.innerProduct(
						userspace[ri.userId], itemspace[ri.itemId]);
				//perform gradient on pu/qi
				for(int f = 0 ; f < this.factor; f++){
					userspace[ri.userId][f] = userspace[ri.userId][f] + learningSpeed * (eui * itemspace[ri.itemId][f] - this.lambda * userspace[ri.userId][f]);
					itemspace[ri.itemId][f] = itemspace[ri.itemId][f] + learningSpeed * (eui * userspace[ri.userId][f] - this.lambda * itemspace[ri.itemId][f]);
				}
				totalError += Math.abs(eui);
				n += 1;
			}
			
		}
		ConcurrentExecutor.get(matrixUpdate);
	}


	@Override
	protected void computeLoop() throws Exception {
		totalError = 0;
		n = 0;
		int numItems = 0;
		long timeStart = System.currentTimeMillis();
		ArrayList<Vector> batchVectors = new ArrayList<Vector>();
		for(Vector v = dataEntry.getNextVector(); v!= null; v = dataEntry.getNextVector()){
			batchVectors.add(v);
			numItems += v.getSize();
			if (numItems > BATCH_SIZE){
				numItems = 0;
				doCompute(batchVectors);
//				System.out.print("*");
				batchVectors.clear();
			}
		}

		long timeSpent = System.currentTimeMillis() - timeStart;
		learningSpeed *= this.convergence;
		System.out.println(String.format("loop:%d\t%d\ttime(ms):%d\tavgerror:%.6f\tnext alpha:%.5f", looping, n , timeSpent, (totalError/n),learningSpeed));

	}

}
