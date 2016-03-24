package org.shanbo.feluca.model;

import gnu.trove.list.array.TFloatArrayList;

public abstract class FloatObjects {
	private float[] array;
	protected FloatObjects(float[] array){
		this.array = array;
	}
	
	abstract int getObjectSize();
	
	abstract FloatObjects copy();
	
	float[] getArray(){
		return array;
	}
	
	public static class OneDegree extends FloatObjects{

		public OneDegree() {
			super(new float[1]);
		}
		
		/**
		 * be careful, this is a ref
		 * @return
		 */
		public float get(){
			return getArray()[0];
		}
		
		public void set(float value){
			getArray()[0] = value;
		}
		
		public String toString(){
			return String.format("%.4f", getArray()[0]);
		}

		@Override
		int getObjectSize() {
			return 1;
		}
		
		@Override
		float[] getArray() {
			return super.getArray();
		}

		@Override
		FloatObjects copy() {
			OneDegree one = new OneDegree();
			return one;
		}
	}
	
	public static class TwoDegree extends FloatObjects{

		protected TwoDegree(int factors) {
			super(new float[factors * 2 + 1 ]);
		}

		@Override
		int getObjectSize() {
			return getArray().length;
		}

		@Override
		FloatObjects copy() {
			TwoDegree twoDegree = new TwoDegree(this.getObjectSize() >> 1);
			return twoDegree;
		}
		
		public String toString(){
			return new TFloatArrayList(getArray()).toString();
		}
		
		public TwoDegree setOne(float w){
			getArray()[0] = w;
			return this;
		}
		
		public float getOne(){
			return getArray()[0];
		}
		
		public TwoDegree setSigmaX(int k, float v){
			 getArray()[(k << 1) + 1] = v;
			 return this;
		}
		
		public float getSigmaX(int k) {
			return getArray()[(k << 1) + 1];
		}

		
		public TwoDegree setSigmaX2(int k, float v){
			 getArray()[(k << 1) + 2] = v;
			 return this;
		}
		public float getSigmaX2(int k) {
			return getArray()[(k << 1) + 2];
		}
		
	}
}
