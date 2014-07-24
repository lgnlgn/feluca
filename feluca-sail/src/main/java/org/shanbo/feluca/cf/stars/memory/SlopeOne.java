package org.shanbo.feluca.cf.stars.memory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;


import org.shanbo.feluca.cf.common.RatingInfo;
import org.shanbo.feluca.cf.common.Recommender;
import org.shanbo.feluca.cf.common.UserRatings;
import org.shanbo.feluca.data.DataEntry;
import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;


public class SlopeOne implements Recommender{
	DataEntry dataEntry;
	protected int maxiid ;
	protected float avgrating; 
	private static final String modelName = "Slope One";


	float[][] diffs = null;
	int[][] coRating = null;


	private void init_space(){
		System.out.println(DataStatistic.MAX_FEATURE_ID + "  :  " + maxiid);
		System.out.println( modelName + " start loading~~~~~");

		diffs = new float[maxiid + 1][];
		coRating = new int[maxiid + 1][];
		for(int i = 0 ; i <= maxiid; i++){
			diffs[i] = new float[maxiid +1];
			coRating[i] = new int[maxiid +1];
		}
		System.out.println("initialize finish");
	}
	

	public void loadData(DataEntry data) throws Exception {
		dataEntry = data;
		maxiid = Utilities.getIntFromProperties(this.dataEntry.getDataStatistic(), DataStatistic.MAX_FEATURE_ID);
		double totalWeight = Utilities.getDoubleFromProperties(this.dataEntry.getDataStatistic(),DataStatistic.SUM_WEIGHTS);
		int totalFeatures = Utilities.getIntFromProperties(this.dataEntry.getDataStatistic(),DataStatistic.TOTAL_FEATURES);
		
		avgrating = (float)(totalWeight / totalFeatures);

	}

	public void train() throws Exception {
		this.init_space();
		UserRatings ur = new UserRatings();
		for(Vector v = dataEntry.getNextVector(); v!= null; v = dataEntry.getNextVector()){
			ur.setVector(v);
			int rates = ur.getItemNum();
			for(int i = 0 ; i < rates-1; i++){
				RatingInfo rii = ur.getRatingByIndex(i);
				int firstItem = rii.itemId;
				double firstRating = rii.rating;
				for(int j = i+1; j < rates; j++){
					RatingInfo rij = ur.getRatingByIndex(j); //actually rii changed
					int secondItem = rij.itemId;
					double secondRating = rij.rating;

					diffs[firstItem][secondItem] += (float)(firstRating - secondRating);
					diffs[secondItem][firstItem] -= (float)(firstRating - secondRating);

					coRating[secondItem][firstItem] += 1;
					coRating[firstItem][secondItem] += 1;
				}
			}
			if (ur.getUid() % 2000 == 0){
				System.out.print(".");
			}
		}
		System.out.println("finish~");
		
	}

	public void setProperties(Properties prop) {

	}

	public Properties getProperties() {
		return null;
	}


	public double predict(UserRatings user, int itemId) throws Exception {
		if (user == null){
			return -1;
		}else{
			double predict = 0;
			int coRate = 0;
			for(RatingInfo ri = user.getNormalNextRating(); ri != null; ri = user.getNormalNextRating()){
				predict += ri.rating * this.coRating[ri.itemId][itemId] - this.diffs[ri.itemId][itemId];
				coRate += this.coRating[ri.itemId][itemId];
			}
			return (float)(predict / coRate);
		}
	}

	


	public void saveModel(String filePath) throws Exception {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this.diffs);
		oos.writeObject(this.coRating);
		oos.close();
		bos.close();
	}

	public void loadModel(String modelPath) throws Exception {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(modelPath));
		ObjectInputStream ois = new ObjectInputStream(bis);
		this.diffs = (float[][])ois.readObject();
		this.coRating = (int[][])ois.readObject();
		ois.close();
		bis.close();
		this.maxiid = this.diffs.length - 1;
	}


	public float[] predict(UserRatings user) throws Exception {
		if (user == null){
			return null;
		}else{
			float[] predicts = new float[this.maxiid + 1];
			int[] coRatingsArray = new int[this.maxiid + 1];
			//for each rated item
			for(RatingInfo ri = user.getNormalNextRating(); ri != null; ri = user.getNormalNextRating()){
				float[] diffOfItem = diffs[ri.itemId];
				int[] coRates = this.coRating[ri.itemId];
				//get co-relation vector 
				for(int i = 0 ; i <= this.maxiid; i++){
					if (coRates[i] != 0){
						predicts[i] += ri.rating * coRates[i] - diffOfItem[i];
						coRatingsArray[i] += coRates[i];
					}
				}
			}
			for(int i=0; i <= this.maxiid; i++){
				if (coRatingsArray[i] != 0){
					predicts[i] /= coRatingsArray[i];
				}
			}
			return predicts;
		}
	}





	public float[] predict(UserRatings user, int[] itemIds) throws Exception {
		if (user == null){
			return null;
		}else{
			float[] predicts = new float[itemIds.length];
			int[] coRatingsArray = new int[itemIds.length];
			
			for(RatingInfo ri = user.getNormalNextRating(); ri != null; ri = user.getNormalNextRating()){
				
				for( int i = 0 ; i < itemIds.length; i++){
					predicts[i] += ri.rating * this.coRating[ri.itemId][itemIds[i]] - this.diffs[ri.itemId][itemIds[i]];
					coRatingsArray[i] += this.coRating[ri.itemId][itemIds[i]];
				}
			}
			for( int i = 0 ; i < itemIds.length; i++){
				if (coRatingsArray[i] != 0){
					predicts[i] /= coRatingsArray[i];
				}else{
					predicts[i] = avgrating;
				}
			}
			return predicts;
		}
	}


//	public Map<Integer, double[]> predict(List<UserRatings> users)
//			throws Exception {
//		Map<Integer, double[]> result = new HashMap<Integer, double[]>();
//		for(UserRatings user : users){
//			double[] predicts = this.predict(user);
//			result.put(user.getUid(), predicts);
//		}
//		return result;
//	}

}
