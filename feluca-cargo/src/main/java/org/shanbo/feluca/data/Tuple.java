package org.shanbo.feluca.data;

import java.util.List;

import org.shanbo.feluca.data.util.NumericTokenizer;

public class Tuple {
	
	public static enum AlignColumn{
		FIRST,
		SECOND,
	}
	
	public static enum TupleType{
		NOT_TUPLE,
		ONLY_TWO_COLUMNS,
		WEIGHT_TYPE;
		
	}
	
	
	int firstColumn;
	int secondColumn;
	
	
	public Tuple(int firstColumn, int secondColumn){
		this.firstColumn = firstColumn;
		this.secondColumn = secondColumn;
	}
	
	public int getFirstColumn() {
		return firstColumn;
	}

	public int getSecondColumn() {
		return secondColumn;
	}
	
	public String payload(){
		return "1";
	}
	
	/**
	 * <b>NOT</b> for FID_ONLY format! Because tuple contains a primary ID
	 * @param tuples
	 * @return
	 */
	public String toVectorFormat(List<Tuple> tuples, AlignColumn alignColumn){
		StringBuilder builder = new StringBuilder();
		if (alignColumn == AlignColumn.FIRST){
			builder.append(tuples.get(0).getFirstColumn());
			for(Tuple tuple : tuples){
				builder.append(" ").append(tuple.getSecondColumn()).append(":").append(tuple.payload());
			}
		}else{
			builder.append(tuples.get(0).getSecondColumn());
			for(Tuple tuple : tuples){
				builder.append(" ").append(tuple.getFirstColumn()).append(":").append(tuple.payload());
			}
		}
		return builder.toString();
	}
	
	/**
	 * NumericTokenizer is about 30% faster than Splitter-NumericParser
	 * @param line
	 */
	public void parseLine(String line){
		NumericTokenizer nt = new NumericTokenizer();
		nt.load(line);
		firstColumn = (Integer)nt.nextNumber();
		secondColumn = (Integer)nt.nextNumber();
	}
	
	public static Tuple convert(TupleType tupleType, String line){
		if (tupleType == TupleType.ONLY_TWO_COLUMNS){
			Tuple t = new Tuple(0, 0);
			t.parseLine(line);
			return t;
		}else if (tupleType == TupleType.WEIGHT_TYPE){
			WeightTuple wt = new WeightTuple(0, 0, 0);
			wt.parseLine(line);
			return wt;
		}else{
			return null;
		}
	}
	
	public static class WeightTuple extends Tuple{

		float weight;
		public WeightTuple(int firstColumn, int secondColumn, float weight) {
			super(firstColumn, secondColumn);
			this.weight = weight;
		}
		@Override
		public String payload() {
			return String.format("%.5f", weight);
		}
		
		/**
		 * NumericTokenizer is about 30% faster than Splitter-NumericParser
		 */
		public void parseLine(String line){
			NumericTokenizer nt = new NumericTokenizer();
			nt.load(line);
			firstColumn = (Integer)nt.nextNumber();
			secondColumn = (Integer)nt.nextNumber();
			Object value = nt.nextNumber();
			weight = value instanceof Float ? (Float)value : (Integer)value;
		}
	}
}	
