package org.shanbo.feluca.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

/**
 * 
 *  @Description pick element from Collection
 *	@author shanbo.liang
 *  @param <T>
 */
public abstract class ElementPicker<T> {

	final ArrayList<T> list = new ArrayList<T>();

	public ElementPicker(Collection<? extends T> coll){
		this.list.addAll(coll);
	}

	public abstract T pick();
	
	public static class RoundRobinPicker<T> extends ElementPicker<T>{

		int i = 0;
		public RoundRobinPicker(Collection<? extends T> coll) {
			super(coll);
		}

		@Override
		public synchronized T pick() {
			
			if (i >= list.size()){
				i = 0;
			}
			int idx = i;
			i+=1;
			return list.get(idx);
		}
	}

	public static class RandomPicker<T> extends ElementPicker<T>{

		Random r = new Random();
		public RandomPicker(Collection<? extends T> coll) {
			super(coll);
		}

		@Override
		public T pick() {
			int i = r .nextInt(list.size());
			return list.get(i);
		}
	}
}
