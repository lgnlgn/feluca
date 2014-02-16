package org.shanbo.feluca.data;

import java.util.ArrayList;
import java.util.List;

/**
 * public use
 * @author lgn
 *
 */
public class BytesPark {
	
	final int SIZE_STEP = 1024*1024; //1m per
	List<byte[]> caches;
	
	public BytesPark(int blocks){
		caches = new ArrayList<byte[]>(blocks);
		for(int i = 0 ;i < blocks;i++)
			caches.add(new byte[2*SIZE_STEP]);
	}
	
	public void enlarge(int part){
		byte[] tmp = new byte[caches.get(part).length + SIZE_STEP];
		System.arraycopy(caches.get(part), 0, tmp, 0, caches.get(part).length);
		this.caches.set(part, tmp);
	}
	
	public byte[] getBytes(int part){
		return caches.get(part);
	}
	
}
