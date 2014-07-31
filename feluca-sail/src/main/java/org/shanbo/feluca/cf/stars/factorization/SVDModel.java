package org.shanbo.feluca.cf.stars.factorization;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.shanbo.feluca.cf.common.RatingInfo;
import org.shanbo.feluca.cf.common.Recommender;
import org.shanbo.feluca.cf.common.UserRatings;
import org.shanbo.feluca.data2.DataEntry;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.paddle.common.Utilities;



public class SVDModel implements Recommender{
	final static String modelName = "SVD model";
	DataEntry dataEntry;
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
	
	UserRatings ur; // a global object


	protected void init_spaces(){
		userspace = new float[maxuid + 1][];
		itemspace = new float[maxiid + 1][];
		double frag = Math.sqrt((avgrating / factor));
		for(int u = 0; u < userspace.length; u++){
			userspace[u] = new float[factor];
			for (int f = 0 ; f < factor; f++){
				userspace[u][f] = (float)(frag * Utilities.randomDouble());
			}
		}
		System.out.println(itemspace.length);
		for(int i = 0; i < itemspace.length; i++){
			itemspace[i] = new float[factor];
			for (int f = 0 ; f < factor; f++){
				itemspace[i][f] = (float)(frag * Utilities.randomDouble()) ;
			}
		}
	}
	
	public void train() throws Exception {
		System.out.println( modelName + " start loading~~~~~");
		System.out.println("maxuid:" + maxuid);
		System.out.println("maxiid:" + maxiid);
		System.out.println("AVGRATING:" + avgrating);
		System.out.println("start training~~~~");
		System.out.println("f:"+ this.factor + " loops:" + this.loops + " alpha:" + this.alpha+ " lambda:"+this.lambda);
		init_spaces();
		
		_train();
		
	}
	
	private void _train() throws Exception {
		float learningSpeed = this.alpha;

		for (int loop = 0; loop < this.loops; loop++){
			dataEntry.reOpen();
			float totalError = 0;
			int n = 0;
			long timeStart = System.currentTimeMillis();
			
			// core computation
			// for each rating
			for(Vector v = dataEntry.getNextVector(); v!= null; v = dataEntry.getNextVector()){
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
			long timeSpent = System.currentTimeMillis() - timeStart;
			learningSpeed *= this.convergence;
			System.out.println(String.format("loop:%d\t%d\ttime(ms):%d\tavgerror:%.6f\tnext alpha:%.5f", loop, n , timeSpent, (totalError/n),learningSpeed));
//			System.out.println("loop " + loop + " finished~  Time spent: " + (timeSpent / 1000.0) + "  next alpha :" + learningSpeed);
		}
		dataEntry.close();
	}


	public void setProperties(Properties prop) {
		alpha = Float.parseFloat(prop.getProperty("alpha", "0.005"));
		lambda = Float.parseFloat(prop.getProperty("lambda", "0.003"));
		convergence = Float.parseFloat(prop.getProperty("convergence", "0.95"));
		loops = Integer.parseInt(prop.getProperty("loops", "10"));
		factor = Integer.parseInt(prop.getProperty("factor", "50"));
	}

	public Properties getProperties() {
		//TODO
		return null;
	}

	protected float predict(int userId, int itemId) throws Exception {
		return Utilities.innerProduct(userspace[userId] , itemspace[itemId]);
	}

	public float[] predict(UserRatings user) throws Exception {
		int[] itemIds = new int[this.maxiid + 1]; 
		for(int i = 0 ; i < itemIds.length; i++){
			itemIds[i] = i;
		}
		return predict(user, itemIds);
	}
	
	public void loadModel(String modelPath) throws IOException, ClassNotFoundException {
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(modelPath));
		ObjectInputStream ois = new ObjectInputStream(bis);
		this.userspace = (float[][])ois.readObject();
		this.itemspace = (float[][])ois.readObject();
		ois.close();
		bis.close();
		this.maxiid = itemspace.length -1;
		this.maxuid = userspace.length -1;
	}

	public void saveModel(String filePath) throws IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this.userspace);
		oos.writeObject(this.itemspace);
		oos.close();
		bos.close();
	}

	public void loadData(DataEntry data) throws Exception {
		this.dataEntry = data;
		maxuid = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_VECTOR_ID);
		maxiid = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_FEATURE_ID);
		avgrating = (float)(Utilities.getDoubleFromProperties(dataEntry.getDataStatistic(),DataStatistic.SUM_WEIGHTS) 
				/ Utilities.getDoubleFromProperties(dataEntry.getDataStatistic(),DataStatistic.TOTAL_FEATURES));
		totalrating = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.TOTAL_FEATURES);
		uids = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.NUM_VECTORS);
		maxRates = Utilities.getIntFromProperties(dataEntry.getDataStatistic(),DataStatistic.MAX_VECTORSIZE);

		
	}


	public float[] predict(UserRatings user, int[] itemIds) throws Exception {
		int uid = user.getUid();
		float[] result = new float[itemIds.length];
		for(int i = 0 ; i < itemIds.length; i++){
			result[i] = predict(uid, itemIds[i]);
		}
		return result;
	}

	public Map<Integer, float[]> predict(List<UserRatings> users)	throws Exception {
		Map<Integer, float[]> result = new HashMap<Integer, float[]>();
		for(UserRatings ur : users){
			result.put(ur.getUid(), predict(ur));
		}
		return result;
	}

}
