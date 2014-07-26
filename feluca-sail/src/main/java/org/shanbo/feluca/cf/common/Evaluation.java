package org.shanbo.feluca.cf.common;

import org.shanbo.feluca.data.DataEntry;
import org.shanbo.feluca.data.DataEntry.RADataEntry;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;

public class Evaluation {
	
	/**
	 * testing RMSE using history 
	 * @param test
	 * @param model
	 * @return
	 * @throws Exception
	 */
	public static double runRMSE(RADataEntry train, DataEntry test, Recommender model) throws Exception{
		double error = 0;
		
		int dbsize = Utilities.getIntFromProperties(test.getDataStatistic(), DataStatistic.TOTAL_FEATURES);
		System.out.println(dbsize);
		int cc = 0;
		test.reOpen();
		train.reOpen();
		while(test.getDataReader().hasNext()){
			long[] offsetArray = test.getDataReader().getOffsetArray();
			for(int o = 0 ; o < offsetArray.length; o++){
				Vector v = test.getDataReader().getVectorByOffset(offsetArray[o]);
				UserRatings ur = new UserRatings(v);
				
				int[] toPredicts = new int[ur.getItemNum()];
				// draw itemid apart
				for(int i = 0 ; i < ur.getItemNum(); i++){
					toPredicts[i] = ur.getRatingByIndex(i).itemId;
				}
				UserRatings history = new UserRatings(train.getVectorById(ur.getUid()));
				// predict in batch mode
				float[] predicts = model.predict(history, toPredicts);
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
			test.getDataReader().releaseHolding();
		}
		test.close();
		train.close();
		return Math.sqrt(error / dbsize);
	}
	
	/**
	 * without cf history
	 * @param test
	 * @param model
	 * @return
	 * @throws Exception
	 */
	public static double runRMSE(DataEntry test, Recommender model) throws Exception{
		double error = 0;
		
		int dbsize = Utilities.getIntFromProperties(test.getDataStatistic(), DataStatistic.TOTAL_FEATURES);
		System.out.println(dbsize);
		int cc = 0;
		test.reOpen();
		while(test.getDataReader().hasNext()){
			long[] offsetArray = test.getDataReader().getOffsetArray();
			for(int o = 0 ; o < offsetArray.length; o++){
				Vector v = test.getDataReader().getVectorByOffset(offsetArray[o]);
				UserRatings ur = new UserRatings(v);
				
				int[] toPredicts = new int[ur.getItemNum()];
				// draw itemid apart
				for(int i = 0 ; i < ur.getItemNum(); i++){
					toPredicts[i] = ur.getRatingByIndex(i).itemId;
				}
				// predict in batch mode
				float[] predicts = model.predict(ur, toPredicts);
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
			test.getDataReader().releaseHolding();
		}
		test.close();
		return Math.sqrt(error / dbsize);
	}
	
}
