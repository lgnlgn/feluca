package org.shanbo.feluca.data;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelDelegator {
	VectorSerDer serDer;
	DistributeTools rpc;
	BytesPark bytesPark;
	
	public ModelDelegator(){
		serDer = new VectorSerDer();
		rpc = new DistributeTools(1);
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
