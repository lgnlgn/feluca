package org.shanbo.feluca.cf.common;

import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Vector.VectorType;



/**
 * Update to a new one by modifying vector and calling {@link #update()} explicitly
 * Since rating for recommendation is in small range, (0, 5],  
 * We use 4 bytes (a int type) to store id(3 bytes, range can be up to 16 million) and rating(1 byte) 
 * @author lgn
 *
 */
public class UserRatings{
	
	private static VectorType ALLOW_VECTOR_TYPE = VectorType.LABEL_FID_WEIGHT;
	
	Vector vector; 
	RatingInfo current;
	int indexOfItem = 0;
	
	public UserRatings(Vector sample){
		this.vector = sample;
		current = new RatingInfo();
		current.userId = sample.getIntHeader();
	}
	
	
	/**
	 * null vector;
	 */
	public UserRatings(){
		current = new RatingInfo();
	}
	
	public void setVector(Vector v){
		if (v.getOutVectorType() !=  ALLOW_VECTOR_TYPE){
			throw new IllegalArgumentException("vector type not allow : only " + ALLOW_VECTOR_TYPE + "  but found:" + v.getOutVectorType());
		}
		vector = v;
		current.userId = vector.getIntHeader();
	}
	
	public int getUid(){
		return vector.getIntHeader();
	}
	
	public int getItemNum(){
		return vector.getSize();
	}
	
	public RatingInfo getNormalNextRating(){
		RatingInfo tmp = getRatingByIndex(indexOfItem);
		indexOfItem += 1;
		return tmp;
	}
	
	/**
	 * not thread safe!
	 * @param idx
	 * @return
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public RatingInfo getRatingByIndex(int idx) throws ArrayIndexOutOfBoundsException{
		if (vector.getSize() <= idx){
			return null;
		}
		//do not create a new RatingInfo object  
		//update rating info by modify Object's members, return it's reference 
//		current.itemId = (this.vector.features[idx] & 0xffffff); //tail 3 bytes for itemid, 
//		if ((vector.features[idx] & 0xf0000000) > 0){  //check whether  
//			current.rating = ((this.vector.features[idx] & 0xf0000000) >>> 28) + 
//								((this.vector.features[idx] & 0x0f000000) >>> 24) * 0.1f;
//		}else{
//			current.rating = this.vector.weights[idx];
//		}
		current.userId = vector.getIntHeader();
		current.itemId = vector.getFId(idx);
		current.rating = vector.getWeight(idx);
		indexOfItem = idx + 1;
		return current;
	}
	
	public void refresh(){
		indexOfItem = 0;
	}
}
