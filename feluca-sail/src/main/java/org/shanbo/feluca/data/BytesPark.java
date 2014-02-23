package org.shanbo.feluca.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * public use
 * @author lgn
 *
 */
public class BytesPark {
	
	final int SIZE_STEP = 1024*1024; //1m per
	byte[] array;
	int arraySize;
	
	int index;
	
	public BytesPark(){
		array = new byte[2 * SIZE_STEP];
	}
	
	public void enlarge(){
		byte[] tmp = new byte[array.length + SIZE_STEP];
		System.arraycopy(array, 0, tmp, 0, array.length);
		this.array = tmp;
	}
	
	public byte[] getBytes(){
		return array;
	}
	
	/**
	 * TODO check carefully
	 * @param in
	 * @throws IOException
	 */
	public void fill(InputStream in) throws IOException{
		int numBytes = in.read(array, 0, array.length);
		for(; numBytes >= array.length; ){
			enlarge();
			numBytes += in.read(array, numBytes, array.length - numBytes);
		}
		this.arraySize = numBytes;
	}
	
	
	/**
	 * be careful, after swallow the index will move to next
	 * @param id
	 * @param value
	 */
	public void swallow(int id, float value){
		
	}
	
	public int arraySize(){
		return arraySize;
	}
	
	/**
	 * 
	 * invoke it in a for-loop, remember your offset
	 * convert bytes to int
	 * @param offset
	 * @return
	 */
	public int yieldIdFrom8Bytes(int offset){
		return 0;
	}
	
	/**
	 * invoke it in a for-loop, remember your offset
	 * @param offset
	 * @return
	 */
	public float yieldValueFrom8Bytes(int offset){
		return 0f;
	}
	
}
