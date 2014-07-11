package org.shanbo.feluca.distribute.model;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.distribute.launch.GlobalConfig;

/**
 * cache a delta model within
 * @author shanbo.liang
 *
 */
public class ModelClient implements Closeable{
	
	DistributeTools rpc;
	PartialModelInClient partialModel;
	
	private boolean stopNow = false;
	
	public ModelClient(GlobalConfig conf){
		partialModel = new PartialModelInClient(conf.getModelServers().size());
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
//		System.out.print("fe");
		partialModel.partitionQueryIds(ids, rpc.caches);
//		System.out.print("tc");
		rpc.fetchModelBack();
//		System.out.print("hM");
		partialModel.deserializeFrom(rpc.caches);
//		System.out.println("odel.");
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
	
	public boolean reachStopCondition(){
		return stopNow;
	}
	
	public void setEarlyStop(){
		stopNow = true;
	}
}
