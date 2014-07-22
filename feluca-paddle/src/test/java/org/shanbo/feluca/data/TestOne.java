package org.shanbo.feluca.data;

import gnu.trove.list.array.TLongArrayList;

import java.util.List;

import org.shanbo.feluca.data.util.CollectionUtil;

public class TestOne {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long[] array = new long[]{1,2,3,4,5,6,7,8};
		List<long[]> splitLongs = CollectionUtil.splitLongs(array, 1, false);
		for(long[] p : splitLongs){
			System.out.println(TLongArrayList.wrap(p).toString());
		}
	}

}
