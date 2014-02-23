package org.shanbo.feluca.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelDelegator {
	VectorSerDer serDer;
	DistributeTools rpc;

	
	public ModelDelegator(int blocks){
		serDer = new VectorSerDer();
		BytesPark[] caches = new BytesPark[blocks];
		for(int i = 0 ; i < blocks; i++){
			caches[i] = new BytesPark();
		}
		rpc = new DistributeTools(caches);
	}
	
	protected void dataSerialize(){
		
	}
	
	protected void dataDeserialize(){
		
	}
	
	protected void querySerialize(){
		
	}
	
	protected void queryDeserialize(){
		
	}
	
	
	public float getValueOfId(int id){
		return 0f;
	}
	
	public void setValueOfId(int id, float value){
		
	}
	
	
}
