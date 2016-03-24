package org.shanbo.feluca.model;

import org.shanbo.feluca.model.FloatObjects.OneDegree;
import org.shanbo.feluca.model.FloatObjects.TwoDegree;

/**
 * using a float window
 * implementation for batch job
 * @author lgn
 *
 * @param <T>
 */
public abstract class FloatSumBarrier<T extends FloatObjects > {
	Object[] values;
	protected FloatWindow window;
	public FloatSumBarrier(int capacity, FloatObjects value){
	
		window = new FloatWindow(capacity, value.getObjectSize());
		values = new Object[capacity];
	
		for(int i = 0 ; i < capacity; i++){
			FloatObjects copy = value.copy();
			values[i] = copy;
			window.init(i, copy.getArray());
		}
	}
	
	public void reset(){
		window.reset();
	}

	
	
	
	public T get(int ith ){
		if (ith >= values.length || ith < 0){
			throw new IndexOutOfBoundsException();
		}
		window.set(ith);
		return (T)(values[ith]);
	}
	
	public abstract void call() ;
	
	public static void main(String[] args) {
		FloatSumBarrier<TwoDegree> aa = new FloatSumBarrier<TwoDegree>
		(10,new TwoDegree(3)){public void call() {}} ;
		aa.get(0).setOne(1).setSigmaX(0, 4).setSigmaX(1, 1);
		aa.get(2).setSigmaX2(0, 2).setSigmaX(0, 1);
		float[][] zip = FloatWindow.clone(aa.window.zip());
		zip[1][0] = 10;
		zip[1][1] = 110;
		zip[1][2] = 1120;
		zip[2][0] = 88;
		zip[2][3] = 88;
		zip[2][6] = 88;
		
		
		System.out.println(FloatWindow.toString(aa.window.zip()));  
		System.out.println(aa.window); 
		aa.window.unzipAndSet(zip);
		System.out.println(aa.window); 
		
		System.out.println("=====");
		FloatSumBarrier<OneDegree> bb = new FloatSumBarrier<OneDegree>
		(10,new OneDegree()) {public void call() {}} ;
		bb.get(0).set(5);
		bb.get(6).set(11);
		System.out.println(bb.window);
		zip = bb.window.zip();
		System.out.println(FloatWindow.toString(zip));
		zip = FloatWindow.clone(zip);
		zip[1][2] = 3;
		zip[1][5] = 31;
		bb.window.unzipAndSet(zip);
		System.out.println(bb.window);
		
	}
}
