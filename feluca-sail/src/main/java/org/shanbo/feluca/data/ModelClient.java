package org.shanbo.feluca.data;

public class ModelClient {

	PartialModel cache;
	
	public byte[] getPartialModel(Object query){
		
		return cache.getPartialModel(query);
	};
	
	public 	boolean updatePartialModel(Object model){
		return cache.updatePartialModel(model);
	}
	
	public float getValueOfId(int id){
		return 0f;
	}
	
	public void setValueOfId(int id, float value){
		
	}
	
}
