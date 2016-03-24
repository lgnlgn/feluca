package org.shanbo.feluca.data2.util;

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
	
	public static void int2Byte(int integer, byte[] dest){
		if (dest ==null || dest.length < 4){
			throw new RuntimeException();
		}
		dest[0] = (byte)((integer & 0xFF000000) >>24);
		dest[1] = (byte)((integer >> 16) & 0xFF);
		dest[2] = (byte)((integer >> 8) & 0xFF); 
		dest[3] = (byte)(integer & 0xFF);
	}
	
	public static int getInt(byte[] bytes){
		return getInt(bytes, 0);
	}
	
	public static void float2Bytes(float f, byte[] bytes){
		int2Byte(Float.floatToIntBits(f),  bytes);
	}
	
	public static float bytes2Float(byte[] bytes){
		return Float.intBitsToFloat(getInt(bytes));
	}
	
	
	public static void main(String[] args) {
		int aa = 546234325;
		byte[] c = new byte[4];
		int2Byte(aa, c);
		int bb = getInt(c);
		System.out.println(aa);
		System.out.println(bb);
		
		float ff = 4.546f;
		float2Bytes(ff, c);
		float ff2 = bytes2Float(c);
		System.out.println(ff2);
	}
}