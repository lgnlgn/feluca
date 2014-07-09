package org.shanbo.feluca.common;

import java.io.File;
import java.util.Properties;


public class Utilities {

	/**
	 * std display array
	 * @param v
	 */
	public static void displayArray(double[] v){
		for (int i = 0 ; i < v.length; i++){
			System.out.print(v[i]+", ");
		}
		System.out.println();
	}
	
	
	/**
	 * convert long to byte[]
	 * @param res
	 * @return
	 */
	public static byte[] long2outputbytes(long res) { 
		byte[] targets = new byte[8];
		//对于输出方向，从左到右
		targets[7] = (byte) (res & 0xff);// 最低位 
		targets[6] = (byte) ((res >> 8) & 0xff);// 
		targets[5] = (byte) ((res >> 16) & 0xff);//  
		targets[4] = (byte) ((res >> 24) & 0xff);// 
		targets[3] = (byte) ((res >> 32) & 0xff);//  
		targets[2] = (byte) ((res >> 40) & 0xff);//  
		targets[1] = (byte) ((res >> 48) & 0xff);// 
		targets[0] = (byte) (res >>> 56);// 最高位,无符号右移。 
		return targets; 
	} 
	
	public static byte[] short2outputbytes(short res){
		byte[] targets = new byte[2];
		targets[1] = (byte) (res & 0xff);// 最低位 
		targets[0] = (byte) (res >>> 8) ;// 
		return targets;
	}
	
	public static byte[] int2outputbytes(int res) { 
		byte[] targets = new byte[4];
		//对于输出方向，从左到右
		targets[3] = (byte) (res & 0xff);// 最低位 
		targets[2] = (byte) ((res >> 8) & 0xff);// 
		targets[1] = (byte) ((res >> 16) & 0xff);//  
		targets[0] = (byte) (res >>> 24) ;// 

		return targets; 
	} 
	
	
	public static long bytes2long(byte[] b) {

		int mask = 0xff;
		int temp = 0;
		int res = 0;
		for (int i = 0; i < 8; i++) {
			res <<= 8;
			temp = b[i] & mask;
			res |= temp;
		}
		return res;
	}

	public static byte[] long2bytes(long num) {
		byte[] b = new byte[8];
		for (int i = 0; i < 8; i++) {
			b[i] = (byte) (num >>> (56 - i * 8));
		}
		return b;
	}

	public static double getDoubleFromSer(int serint){
		int head = (serint & 0xf0000000) >>> 28; 
		int tail = (serint & 0xf000000) >> 24;
		return  head + tail /10.0; 
	}
	
//	public static TreeMap<Integer, Double> parseVector(String vectorString){
//		String[] info = vectorString.split("\\s+");
//		TreeMap<Integer, Double> vec = new TreeMap<Integer, Double>();
//		for(String str : info){
//			String[] kw = str.split(":");
//			vec.put(Integer.parseInt(kw[0]), Double.parseDouble(kw[1]));
//		}
//		return vec;
//	}
	
	public static int getIntFromProperties(Properties p, String key){
		return Integer.parseInt((String)(p.getProperty(key)));
	}
	
	public static String getStrFromProperties(Properties p, String key){
		return (String)(p.getProperty(key));
	}
	
	public static double getDoubleFromProperties(Properties p, String key){
		return Double.parseDouble((String)(p.getProperty(key)));
	}
	
	public static byte[] float2OutputBytes(float input){
		int data=Float.floatToIntBits(input);
		int j = 0;
		byte[] outData=new byte[4];
		outData[j++]=(byte)(data>>>24);
		outData[j++]=(byte)(data>>>16);
		outData[j++]=(byte)(data>>>8);
		outData[j++]=(byte)(data>>>0);
		return outData;
	}
	
	public static String getAbsFilePrefixPath(String absPath){
		File f = new File(absPath);
		String name = f.getName();
		int idx = name.lastIndexOf(".");
		return f.getParent() + File.separator + name.substring(0, idx);
	}
	
//	public static String RAMEstimation(CommandLine cmd, Properties parameters, MemoryEstimater algo, String dbPath) throws IOException{
//		if (cmd.hasOption("f"))
//			parameters.put(Constants.FILESTREAM_INPUT, "");
//		Properties data = new Properties();
//		data.load(new FileInputStream(new File(dbPath + Constants.STAT_SUFFIX)));
//		int kb = algo.estimate(data, parameters);
//		return String.format("Algorithm memory usage approximate : %.1f MB (not include JVM)" , kb / 1024.0);
//	}
//	
	
}
