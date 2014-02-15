package org.shanbo.feluca.data;

/**
 * cache a delta model within? or put it outside
 * @author shanbo.liang
 *
 */
public class PartialModel {
	VectorSerDer serDer;
	DistributeTools rpc;
	Object model;
	Object delta;
	
	public byte[] getPartialModel(Object query){
		byte[] serializedQuery = serDer.serializeQuery(query);
		//TODO
		return null;
	};
	
	public 	boolean updatePartialModel(Object model){
		byte[] serializedModel = serDer.serializePartialModel(model);
		byte[] result = rpc.request("/update", serializedModel);
		if (new String(result).equals("200"))
			return true;
		else {
			return false;
		}
	}
}
