package org.shanbo.feluca.data2.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class CollectionUtil {

	public static Random random = new Random();
	
	public static int[] shuffle(int[] array){
		
		for (int i=0; i<array.length; i++) {
			int randomPosition = random.nextInt(array.length);
			int temp = array[i];
			array[i] = array[randomPosition];
			array[randomPosition] = temp;
		}

		return array;
	}
	
	public static void shuffle(long[] array, int startIndex, int endIndex){
		assert (endIndex <= array.length && startIndex >= 0 && endIndex > 0);
		for (int i= startIndex; i<endIndex; i++) {
			int randomPosition = random.nextInt(endIndex - startIndex) + startIndex;
			long temp = array[i];
			array[i] = array[randomPosition];
			array[randomPosition] = temp;
		}

	}
	
	public static List<long[]> splitLongs(long[] offsetArray, int numPerBlock, boolean shuffled){
		List<long[]> result = new ArrayList<long[]>(offsetArray.length / numPerBlock + 1);
		long[] tmp = Arrays.copyOf(offsetArray, offsetArray.length);
		if (shuffled){
			CollectionUtil.shuffle(tmp, 0, tmp.length);
		}
		int i = 0;
		for( ; i < offsetArray.length / numPerBlock; i++){
			result.add(Arrays.copyOfRange(tmp, i * numPerBlock, (i+1) * numPerBlock));
		}
		if ( (i) * numPerBlock < tmp.length){
			result.add(Arrays.copyOfRange(tmp, i * numPerBlock, tmp.length));
		}
		return result;
	}
	
}