package org.shanbo.feluca.data;

public class ModelClient {

	ModelDelegator cache;
	
	public void init(){
		cache = new ModelDelegator(1);
	}
	
	public ModelClient(){
		
	}
	
	public ModelDelegator getPartialModel(){
		return cache;
	}
	
	/**
	 * upload
	 */
	public void updateModel(){
		
	}
	
	/**
	 * download
	 */
	public void syncModel(){
		
	}
	
	
	
}
