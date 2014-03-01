package org.shanbo.feluca.data;

import java.util.concurrent.ExecutionException;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelClient {
	
	DistributeTools rpc;
	PartialModelInClient partialModel;
	
	public ModelClient(GlobalConfig conf){
		partialModel = new PartialModelInClient(conf.nodes());
		rpc = new DistributeTools(conf);
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
		
	
}
