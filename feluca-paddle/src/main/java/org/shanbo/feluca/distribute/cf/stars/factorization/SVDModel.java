package org.shanbo.feluca.distribute.cf.stars.factorization;

import gnu.trove.set.hash.TIntHashSet;

import java.util.List;

import org.shanbo.feluca.cf.common.RatingInfo;
import org.shanbo.feluca.cf.common.UserRatings;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.data.util.CollectionUtil;
import org.shanbo.feluca.distribute.launch.GlobalConfig;
import org.shanbo.feluca.distribute.launch.LoopingBase;
import org.shanbo.feluca.distribute.model.PartialMatrixModel;
import org.shanbo.feluca.paddle.common.Utilities;
import org.shanbo.feluca.util.JSONUtil;

import com.alibaba.fastjson.JSONObject;

public class SVDModel extends LoopingBase{

	final static String USER_MATRIX = "userSpace";
	final static String ITEM_MATRIX = "itemSpace";
	
	protected PartialMatrixModel userSpace;
	protected PartialMatrixModel itemSpace;
	
	protected JSONObject parameters;
	protected JSONObject dataStatistic;
	
	protected float alpha = 0.003f;
	protected float lambda = 0.001f;
	protected float convergence = 0.90f;
	protected int factor = 50;
	
	protected double totalError = 0;
	protected int n = 0;
	protected float learningSpeed ;
	protected long tStart ;
	protected long tEnd;
	
	public SVDModel(GlobalConfig conf) throws Exception {
		super(conf);
		this.parameters = conf.getAlgorithmConf();
		this.dataStatistic = conf.getDataStatistic();
		this.factor = JSONUtil.getConf(parameters, "factor", 50);
		this.lambda = JSONUtil.getConf(parameters, "lambda", 0.0015f);
		this.alpha = JSONUtil.getConf(parameters, "alpha", 0.003f);	
		this.convergence = JSONUtil.getConf(parameters, "convergence", 0.95f);
		this.learningSpeed = alpha;
	}

	@Override
	protected void modelStart() throws Exception {
		float avgrating = dataStatistic.getFloatValue(DataStatistic.SUM_WEIGHTS) 
				/ dataStatistic.getFloatValue(DataStatistic.TOTAL_FEATURES);
		float vibration = (float)(Math.sqrt(avgrating / this.parameters.getIntValue("factors")));
		
		modelClient.createMatrix(USER_MATRIX, dataStatistic.getIntValue(DataStatistic.MAX_VECTOR_ID) + 1,
				this.parameters.getIntValue("factors"), 0, vibration);
		modelClient.createMatrix(ITEM_MATRIX, dataStatistic.getIntValue(DataStatistic.MAX_FEATURE_ID) + 1, 
				this.parameters.getIntValue("factors"), 0, vibration);
	}

	@Override
	protected void modelClose() throws Exception {
		modelClient.dumpMatrix(USER_MATRIX, Constants.Base.getWorkerRepository() + "/model/" + conf.getAlgorithmName());
		modelClient.dumpMatrix(ITEM_MATRIX, Constants.Base.getWorkerRepository() + "/model/" + conf.getAlgorithmName());
	}

	protected void computeLoopBegin() throws Exception {
		totalError = 0;
		n = 0;
		tStart = System.currentTimeMillis();
	}
	
	protected void computeLoopEnd() {
		tEnd = System.currentTimeMillis();
		learningSpeed *= parameters.getFloatValue("convergence");
		System.out.println(String.format("loop:%d\t%d\ttime(ms):%d\tavgerror:%.6f\tnext alpha:%.5f", loops, n , (tEnd - tStart), (totalError/n),learningSpeed));
	}
	
	@Override
	protected void computeBlock() throws Exception {
		long[] offsetArray = dataReader.getOffsetArray(); 
		List<long[]> splitted = CollectionUtil.splitLongs(offsetArray, 1000, false);
		//continue split 1000 per block
		for(long[] segment : splitted){
			TIntHashSet fidSet = new TIntHashSet(); //items
			TIntHashSet vidSet = new TIntHashSet(); //users
			for(long offset : segment){ 
				Vector v = dataReader.getVectorByOffset(offset);
				distinctIds(fidSet, v);    //itemids
				vidSet.add(v.getIntHeader()); //userid
			}
			int[] itemIds = fidSet.toArray();
			int[] userIds = vidSet.toArray();
			modelClient.matrixRetrieve(ITEM_MATRIX, itemIds);
			modelClient.matrixRetrieve(USER_MATRIX, userIds);
			//gradient
			for(long offset : segment){ 
				Vector v = dataReader.getVectorByOffset(offset);
				UserRatings ur = new UserRatings(v);
				float[] userspace = userSpace.get(ur.getUid());
				for(RatingInfo ri = ur.getNormalNextRating(); ri != null ; ri = ur.getNormalNextRating()){
					float[] itemspace = itemSpace.get(ri.itemId);
					float eui = ri.rating - Utilities.innerProduct(	userspace, itemspace);
					//perform gradient on pu/qi
					for(int f = 0 ; f < this.factor; f++){
						userspace[f] = userspace[f] + learningSpeed * (eui * itemspace[f] - this.lambda * userspace[f]);
						itemspace[f] = itemspace[f] + learningSpeed * (eui * userspace[f] - this.lambda * itemspace[f]);
					}
					totalError += Math.abs(eui);
					n += 1;
				}
			}
			modelClient.matrixUpdate(ITEM_MATRIX, userIds);
			modelClient.matrixUpdate(USER_MATRIX, userIds);
			System.out.print("!");
		}
	}

}
