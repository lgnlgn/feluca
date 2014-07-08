package org.shanbo.feluca.distribute.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * public use
 * @author lgn
 *
 */
public class BytesPark {
	
	final int SIZE_STEP = 256 * 1024; //1m per
	byte[] array;
	
	int startIndex;
	int endIndex;
	
	int arraySize;

	
	
	public BytesPark(){
		arraySize = 0;
		array = new byte[2 * SIZE_STEP];

	}
	
	public void enlarge(){
		System.out.println("!enlarge");
		byte[] tmp = new byte[array.length + SIZE_STEP];
		System.arraycopy(array, 0, tmp, 0, array.length);
		this.array = tmp;
	}
	
	public byte[] getArray(){
		return array;
	}
	
	/**
	 * set arraySize = 0
	 */
	public void clear(){
		this.arraySize = 0;
	}
	
	
	
	/**
	 * TODO use carefully, start from 0,will erase entire array
	 * @param in
	 * @throws IOException
	 */
	public void fill(InputStream in) throws IOException{
		int numBytes = in.read(array, 0, array.length);
		for(; numBytes >= array.length; ){
			enlarge();
			int read =  in.read(array, numBytes, array.length - numBytes);
			if (read > 0){
				numBytes += read;
			}else{
				break;
			}
		}
		this.arraySize = numBytes;
	}
	
	/**
	 * you must ensure the capacity
	 * @param offset 8 bytes per position
	 * @param id
	 * @param value
	 * @param array
	 */
	public static void fillIntFloatToBytes(int offset, int id, float value, byte[] array){
		array[offset+0] = (byte)((id >> 24) & 0xff);
		array[offset+1] = (byte)((id >> 16) & 0xff);
		array[offset+2] = (byte)((id >> 8) & 0xff);
		array[offset+3] = (byte)((id ) & 0xff);
		int f = Float.floatToIntBits(value);
		array[offset+4] = (byte)((f >> 24) & 0xff);
		array[offset+5] = (byte)((f >> 16) & 0xff);
		array[offset+6] = (byte)((f >> 8) & 0xff);
		array[offset+7] = (byte)((f) & 0xff);
	}
	
	/**
	 * be careful, after swallowed the arraySize will increase 8
	 * @param id
	 * @param value
	 */
	public void swallow(int id, float value){
		if (arraySize == array.length){
			enlarge();
		}	
		array[arraySize++] = (byte)((id >> 24) & 0xff);
		array[arraySize++] = (byte)((id >> 16) & 0xff);
		array[arraySize++] = (byte)((id >> 8) & 0xff);
		array[arraySize++] = (byte)((id ) & 0xff);
		int f = Float.floatToIntBits(value);
		array[arraySize++] = (byte)((f >> 24) & 0xff);
		array[arraySize++] = (byte)((f >> 16) & 0xff);
		array[arraySize++] = (byte)((f >> 8) & 0xff);
		array[arraySize++] = (byte)((f) & 0xff);
	}
	
	/**
	 * be careful, after swallowed the arraySize will increase 4
	 * @param id
	 * @param value
	 */
	public void swallowInt(int id){
		if (arraySize == array.length){
			enlarge();
		}
		array[arraySize++] = (byte)((id >> 24) & 0xff);
		array[arraySize++] = (byte)((id >> 16) & 0xff);
		array[arraySize++] = (byte)((id >> 8) & 0xff);
		array[arraySize++] = (byte)((id ) & 0xff);
	}
	
	public int arraySize(){
		return arraySize;
	}
	
	public int capacity(){
		return array.length;
	}
	
	
	
	/**
	 * use by server side 
	 * @param offset
	 * @return
	 */
	public static int yieldIdFrom4bytes(int offset, byte[] array){
		int id = 0;
		id |= ((array[offset] & 0xff) << 24);
		id |= ((array[offset+1] & 0xff) << 16);
		id |= ((array[offset+2] & 0xff) << 8);
		id |= ((array[offset+3] & 0xff) );
		return id;
	}
	
	/**
	 * 
	 * invoke it in a for-loop, remember your offset
	 * convert bytes to int
	 * @param offset
	 * @return
	 */
	public static int yieldIdFrom8Bytes(int offset, byte[] array){
		int id = 0;
		id |= ((array[offset] & 0xff) << 24);
		id |= ((array[offset+1] & 0xff) << 16);
		id |= ((array[offset+2] & 0xff) << 8);
		id |= ((array[offset+3] & 0xff) );
		return id;
	}
	
	/**
	 * 
	 * invoke it in a for-loop, remember your offset
	 * convert bytes to int
	 * @param offset
	 * @return
	 */
	public  int extractIdEach8Bytes(int offset){
		return yieldIdFrom8Bytes(offset, array);
	}
	
	/**
	 * invoke it in a for-loop, remember your offset
	 * @param offset
	 * @return
	 */
	public static float yieldValueFrom8Bytes(int offset, byte[] array){
		int id = 0;
		id |= ((array[offset + 4] & 0xff) << 24);
		id |= ((array[offset+5] & 0xff) << 16);
		id |= ((array[offset+6] & 0xff) << 8);
		id |= ((array[offset+7] & 0xff) );
		return Float.intBitsToFloat(id);
	}
	
	/**
	 * invoke it in a for-loop, remember your offset
	 * @param offset
	 * @return
	 */
	public  float extractValueEach8Bytes(int offset){
		return yieldValueFrom8Bytes(offset, array);
	}
	
	
	public static void main(String[] args) throws IOException {
		BytesPark park = new BytesPark();
//		park.swallow(Integer.MAX_VALUE, -0.5f);
//		park.swallow(5, -6.5f);
//		System.out.println(park.yieldIdFrom8Bytes(0) + " : " + park.yieldValueFrom8Bytes(0));
//		System.out.println(park.yieldIdFrom8Bytes(8) + " : " + park.yieldValueFrom8Bytes(8));
//		System.out.println(park.arraySize());
//		long t = System.nanoTime();
//		for(int i = 0 ; i < 1000000; i++){
//			park.yieldIdFrom8Bytes(0);
//		}
//		long t2 = System.nanoTime();
//		System.out.println("cost:" + (t2-t));
		

		park.swallow(1, 1.5f);
		park.swallow(5, 0.5f);
		park.swallow(5, 0.5f);
		park.swallow(5, 0.5f);
		park.swallow(5, 0.5f);
		park.swallow(5, 0.5f);
		System.out.println(park.arraySize + "   " );
		byte[] arr = new byte[64];
		Arrays.fill(arr, (byte)67);
		InputStream in = new ByteArrayInputStream(arr);
		park.fill(in);
		System.out.println(park.arraySize + "   ");
		for(int i = 0 ; i < park.arraySize;i++){
			System.out.println(park.array[i]);
		}
		
		System.out.println(park.arraySize());
		long t = System.nanoTime();
		for(int i = 0 ; i < 1000000; i++){
			park.swallow(5, 0.5f);
		}
		long t2 = System.nanoTime();
		System.out.println("cost:" + (t2-t));
		
	}
}
