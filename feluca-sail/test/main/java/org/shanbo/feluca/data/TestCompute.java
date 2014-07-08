package org.shanbo.feluca.data;

import java.util.Arrays;
import java.util.Random;

import java.util.Random;

public class TestCompute {
	static int[] f = new int[129];
	
	static{
		Arrays.fill(f, -1);
		for(int i = 0 ; i <= 8; i++){
			int index = (int) Math.pow(2, i);
			f[index] = i;
			if (index >= 128){
				break;
			}
		}
	}
	
	static Random r = new Random();
	static void cal(int[] list, int i){
		for(int ii = 0; ii < list.length;ii++){
//			if (i == 1){
//				list[ii]= list[ii] >> 0;
//			}else if (i == 2){
//				list[ii]= list[ii] >> 1;
//			}else{
//				list[ii]= list[ii] / i;
//			}
			int off = f[i];
			if (off < 0){
				list[ii] = list[ii]/ i;
			}else{
				list[ii] = list[ii] >> off;
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int[] array = new int[1000000];
		for(int i = 0 ; i < array.length; i++){
			array[i] = r.nextInt(1000000);
		}
		long t = System.currentTimeMillis();
		for(int i = 0 ; i < 1000; i++){
			cal(array, 2);
		}
		System.out.println(System.currentTimeMillis() - t);
	}

}
