package org.shanbo.feluca.data.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.shanbo.feluca.data.Tuple;
import org.shanbo.feluca.data.Tuple.AlignColumn;
import org.shanbo.feluca.data.Tuple.TupleType;

public class TextReader {
	BufferedReader reader;
	boolean isTupleLine;
	NumericTokenizer lineParser;
	List<Tuple> tuples;
	int lastColumn = Integer.MIN_VALUE;
	Tuple.AlignColumn alignColumn;
	Tuple.TupleType tupleType;
	
	public TextReader(BufferedReader reader, Tuple.TupleType tupleType, Tuple.AlignColumn  alignColumn){
		this.reader = reader;
		this.tupleType = tupleType;
		lineParser = new NumericTokenizer();
		tuples = new ArrayList<Tuple>();
		this.alignColumn = alignColumn;
	}

	public TextReader(BufferedReader reader){
		this(reader, TupleType.NOT_TUPLE, Tuple.AlignColumn.FIRST);
	}

	public String readLine() throws IOException{
		if (tupleType == TupleType.NOT_TUPLE){
			return reader.readLine();
		}else{
			for(String line = reader.readLine(); line!= null; line = reader.readLine() ){
				Tuple newTuple = Tuple.convert(this.tupleType, line);
				if (isSameAlignAndSet(newTuple)){
					tuples.add(newTuple);
				}else{
					if (tuples.isEmpty()){ //first line
						continue;
					}
					String merged = mergeTuplesToLine(tuples);
					tuples.add(newTuple);
					return merged;
				}
			}
			if (tuples.isEmpty()){
				return null;
			}
			String last = mergeTuplesToLine(tuples);
			return last;
		}

	}

	private String mergeTuplesToLine(List<Tuple> tuples){
		StringBuilder builder = new StringBuilder();
		if (this.alignColumn == AlignColumn.FIRST){
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
		tuples.clear();
		return builder.toString();
	}

	private boolean isSameAlignAndSet(Tuple tuple){
		int columnCompare = (alignColumn == AlignColumn.FIRST) ? tuple.getFirstColumn() : tuple.getSecondColumn();
		if (columnCompare == lastColumn){
			this.lastColumn = columnCompare;
			return true;
		}else {
			this.lastColumn = columnCompare;
			return false;
		}
		
	}

	
	public void close() throws IOException{
		reader.close();
	}
}
