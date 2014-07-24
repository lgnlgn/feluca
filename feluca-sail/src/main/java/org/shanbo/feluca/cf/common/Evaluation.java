package org.shanbo.feluca.cf.common;

import org.shanbo.feluca.data.DataEntry;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;

public class Evaluation {
	/**
	 * testing RMSE using batch prediction 
	 * @param data
	 * @param recommender
	 * @return
	 * @throws Exception
	 */
	public static double runRMSE(DataEntry data, Recommender recommender) throws Exception{
		double error = 0;
		UserRatings ur = new UserRatings();
		int dbsize = Utilities.getIntFromProperties(data.getDataStatistic(), DataStatistic.TOTAL_FEATURES);
		System.out.println(dbsize);
		int cc = 0;
		data.reOpen();
		while(data.getDataReader().hasNext()){
			long[] offsetArray = data.getDataReader().getOffsetArray();
			for(int o = 0 ; o < offsetArray.length; o++){
				Vector v = data.getDataReader().getVectorByOffset(offsetArray[o]);
				ur.setVector(v);
				
				int[] itemIds = new int[ur.getItemNum()];
				// draw itemid apart
				for(int i = 0 ; i < ur.getItemNum(); i++){
					itemIds[i] = ur.getRatingByIndex(i).itemId;
				}
				// predict in batch mode
				float[] predicts = recommender.predict(ur, itemIds);
				if (predicts == null)
					dbsize -= ur.getItemNum();
				else{
					for(int i = 0 ; i < ur.getItemNum(); i++ ){
						RatingInfo ri = ur.getRatingByIndex(i);
						error += Math.pow((ri.rating - predicts[i]), 2);
					}
				}
				cc += 1;
				if (cc % 2000 == 0){
					System.out.print(".");
				}
			}
			data.getDataReader().releaseHolding();
		}

		return Math.sqrt(error / dbsize);
	}
}
