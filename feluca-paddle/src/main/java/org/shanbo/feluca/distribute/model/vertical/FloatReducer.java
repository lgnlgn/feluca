package org.shanbo.feluca.distribute.model.vertical;

/**
 *  it's not for concurrent computation; call it in batch way 
 * @author lgn
 *
 */
public interface FloatReducer {
	
	public final static int PORT_AWAY = 100;
	
	public float[] reduce(String name, int clientId, float[] orderValues);
	
	public String getName();
}
