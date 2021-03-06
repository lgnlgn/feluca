package org.shanbo.feluca.cf.common;


import java.util.Properties;

import org.shanbo.feluca.data2.DataEntry;

/**
 * a recommender do not maintain the user history 
 * in order to predict for user you need to fetch user model by yourself
 * @author lgn
 *
 */
public interface Recommender {
	
	public void loadData(DataEntry data) throws Exception;
	
	public void train() throws Exception;
		
	public void setProperties(Properties prop);
	
	public Properties getProperties();

	
	/**
	 * Predict a user's all-items rating, using his history. 
	 * @param user
	 * @return
	 * @throws Exception
	 */
	public float[] predict(UserRatings user)  throws Exception;
	
	
	/**
	 * batch prediction of {@link #predict(UserRatings)}
	 * @param userId
	 * @param itemIds
	 * @return
	 * @throws Exception
	 */
	public float[] predict(UserRatings user, int[] itemIds) throws Exception;
	
	
	/**
	 * multi-user prediction of {@link #predict(UserRatings)}}, often for fast testing
	 * @param userId
	 * @param itemId
	 * @return
	 * @throws Exception
	 */
//	public Map<Integer, float[]> predict(List<UserRatings> users) throws Exception;
	
	public void saveModel(String filePath) throws Exception;
	
	public void loadModel(String modelPath) throws Exception;
	
	
}
