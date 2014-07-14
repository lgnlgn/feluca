package org.shanbo.feluca.distribute.newmodel;

/**
 * general vector db; partition global vector before create it
 * @author lgn
 *
 */
public interface VectorDB {
	public void createVector(String collName, int vectorSize, float defaultValue, boolean overwrite);
	
	/**
	 * id will be array's index; in server side, a more compact structure should be used;
	 * @param collName
	 * @param ids
	 * @return
	 */
	public float[] multiGet(String collName, int[] ids);
	
	/**
	 * id will be array's index; in server side, a more compact structure should be used;
	 * update delta values ; not just erase them
	 * @param collName
	 * @param ids
	 * @return
	 */
	public void multiUpdate(String collName, int[] ids, float[] deltaValues);
	
	public void dumpToDisk(String collName, String path, int maxShard, int shardId);
}
