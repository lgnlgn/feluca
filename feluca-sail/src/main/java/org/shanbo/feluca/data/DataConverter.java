package org.shanbo.feluca.data;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.shanbo.feluca.data.Vector.VectorType;

import com.google.common.io.Closeables;

public class DataConverter {
	ByteBuffer buffer;
	
	Vector vector;
	
	BufferedOutputStream out;
	BufferedWriter statWriter;
	BufferedReader rawDataReader;
	
	public DataConverter(String inFile) throws FileNotFoundException{
		rawDataReader = new BufferedReader(new FileReader(inFile));
	}
	
	/**
	 * convert fim data format
	 * @param outDir
	 * @throws IOException
	 */
	public void convertFID2FID(String outDir) throws IOException{
		File dir = new File(outDir);
		if (dir.isFile()){
			dir.delete();
		}
		if (!dir.exists()){
			dir.mkdir();
		}
		String dataName = dir.getName();
		vector = Vector.build(VectorType.FIDONLY);
		vector.setOutputType(VectorType.FIDONLY);
		buffer = ByteBuffer.wrap(new byte[32 * 1024 * 1024]);
		int blockId = 0;
		int partOfBlock = 2; // trim to size when this > 1;
		DataStatistic statistic = new DataStatistic.BasicStatistic();
		
		statWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_1.sta"));
		out = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_1.dat" ));

		for(String line = rawDataReader.readLine(); line != null ; line = rawDataReader.readLine()){
			vector.parseLine(line);
			statistic.stat(vector);
			boolean success = vector.appendToByteBuffer(buffer);
			if (success == false){
				if (partOfBlock >= 2){
					statWriter.write(statistic.toString()); //finish stat of this block
					Closeables.close(out, true); //close old and open a new
					Closeables.close(statWriter, false);
					blockId += 1;
					out = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".dat" ));
					statWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".sta"));
					partOfBlock = 0;
				}
				for(;  buffer.position() < buffer.capacity(); ){
					buffer.putInt(0);
				}
				out.write(buffer.array());
				buffer.clear();
				partOfBlock += 1;
			}
		}
		
//		System.out.println(buffer.position());
//		System.out.println(buffer.arrayOffset());
//		System.out.println(buffer.capacity());
		//end of 
		out.write(buffer.array(), 0, buffer.position());
		statWriter.write(statistic.toString());
		
		Closeables.close(out, true);
		Closeables.close(rawDataReader, true);
		Closeables.close(statWriter, true);
	}

	
}
