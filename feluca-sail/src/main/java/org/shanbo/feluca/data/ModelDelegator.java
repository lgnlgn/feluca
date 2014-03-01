package org.shanbo.feluca.data;

import java.util.concurrent.ExecutionException;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelDelegator {
	
	DistributeTools rpc;
	ModelInClient partialModel;
	
	public ModelDelegator(int blocks){
		partialModel = new ModelInClient(blocks);
		BytesPark[] caches = new BytesPark[blocks];
		for(int i = 0 ; i < blocks; i++){
			caches[i] = new BytesPark();
		}
		rpc = new DistributeTools(caches);
	}
	
	public void updateModel(int ids[]) throws InterruptedException, ExecutionException{
		for(int i = 0 ; i < rpc.caches.length; i++){
			rpc.caches[i].clear();
		}
		partialModel.partitionAndSerialize(ids, rpc.caches);
		rpc.updateModel();
	}
	
	public void fetchModel(int ids[]) throws InterruptedException, ExecutionException{
		for(int i = 0 ; i < rpc.caches.length; i++){
			rpc.caches[i].clear();
		}
		partialModel.partitionQueryIds(ids, rpc.caches);
		rpc.fetchModelBack();
		partialModel.deserializeFrom(rpc.caches);
	}
	
	protected void dataDeserialize(){
		
	}
	
	protected void querySerialize(){
		
	}
	
	protected void queryDeserialize(){
		
	}
	
	
	
	
	
}
