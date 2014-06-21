package org.shanbo.feluca.data.convert;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class ConvertJob {

	final static Set<String> actionSet = ImmutableSet.of("1", "2");
	
	public static void LW2LW(String raw, String out) throws IOException{
		DataConverter dc = new DataConverter(raw);
		dc.convertLW2LW(out);
	}
	
	public static void FID2FID(String raw, String out) throws IOException{
		DataConverter dc = new DataConverter(raw);
		dc.convertFID2FID(out);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String action = args[0];
		String rawDataPath = args[1];
		String outputDir = args[2];
		if (actionSet.contains(action)){
			if (action.equals("1")){
				LW2LW(rawDataPath, outputDir);
			}else if (action.equals("2")){
				FID2FID(rawDataPath, outputDir);
			}
		}else{
			System.out.println("action list");
			System.out.println("1 : label weight -> label weight ");
			System.out.println("2 : fid -> fid ");
		}
	}

}
