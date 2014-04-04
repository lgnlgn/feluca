package org.shanbo.feluca.data.util;

public class BytesUtil {
	public static int getInt(byte[] array, int pos){
		int id = 0;
		id |= ((array[pos] & 0xff) << 24);
		id |= ((array[pos+1] & 0xff) << 16);
		id |= ((array[pos+2] & 0xff) << 8);
		id |= ((array[pos+3] & 0xff) );
		return id;
	}
	
	public static float getFloat(byte[] array, int pos){
		return Float.intBitsToFloat(getInt(array, pos));
	}
	
	
}
