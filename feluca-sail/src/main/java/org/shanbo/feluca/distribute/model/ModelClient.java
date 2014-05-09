package org.shanbo.feluca.distribute.model;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelClient implements Closeable{
	
	DistributeTools rpc;
	PartialModelInClient partialModel;
	
	public ModelClient(GlobalConfig conf){
		partialModel = new PartialModelInClient(conf.nodes());
		rpc = new DistributeTools(conf);
	}
	
	
	public void close() throws IOException{
		rpc.close();
	}
	
	/**
	 * update model to remote , needs partition model by ids according to partitioner
	 * @param ids
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void updateModel(int ids[]) throws InterruptedException, ExecutionException{
		for(int i = 0 ; i < rpc.caches.length; i++){
			rpc.caches[i].clear();
		}
		partialModel.partitionAndSerialize(ids, rpc.caches);
		rpc.updateModel();
	}
	
	/**
	 * fetchModelBack by ids
	 * @param ids
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void fetchModel(int ids[]) throws InterruptedException, ExecutionException{
		for(int i = 0 ; i < rpc.caches.length; i++){
			rpc.caches[i].clear();
		}
		partialModel.partitionQueryIds(ids, rpc.caches);
		rpc.fetchModelBack();
		partialModel.deserializeFrom(rpc.caches);
	}
		
	public float getById(int id){
		return partialModel.getById(id);
	}
	
	/**
	 * set new value, not just delta
	 * @param id
	 * @param value
	 * @return
	 */
	public void setValue(int id, float value){
		partialModel.setValue(id, value);
	}
}
