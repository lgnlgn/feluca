package org.shanbo.feluca.data;

import org.apache.commons.lang.math.RandomUtils;

public class CollectionUtil {

	public static int[] shuffle(int[] array){
		
		for (int i=0; i<array.length; i++) {
			int randomPosition = RandomUtils.nextInt(array.length);
			int temp = array[i];
			array[i] = array[randomPosition];
			array[randomPosition] = temp;
		}

		return array;
	}
	
	public static void shuffle(long[] array, int startIndex, int endIndex){
		assert (endIndex <= array.length && startIndex >= 0 && endIndex > 0);
		for (int i= startIndex; i<endIndex; i++) {
			int randomPosition = RandomUtils.nextInt(endIndex - startIndex) + startIndex;
			long temp = array[i];
			array[i] = array[randomPosition];
			array[randomPosition] = temp;
		}

	}
}
