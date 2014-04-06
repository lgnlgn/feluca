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
import org.shanbo.feluca.data.DataStatistic.LableWeightStatistic;
import org.shanbo.feluca.data.DataStatistic.BasicStatistic;

import com.google.common.io.Closeables;

public class DataConverter {
	ByteBuffer buffer;
	
	Vector vector;
	
	BufferedOutputStream out;
	BufferedWriter statWriter;
	BufferedReader rawDataReader;
	
	public DataConverter(String inFile) throws FileNotFoundException{
		rawDataReader = new BufferedReader(new FileReader(inFile));
		buffer = ByteBuffer.wrap(new byte[32 * 1024 * 1024]);
	}
	
	private void generalConverting(String outDir, VectorType inputType, VectorType outpuType, DataStatistic statistic) throws FileNotFoundException, IOException{
		File dir = new File(outDir);
		if (dir.isFile()){
			dir.delete();
		}
		if (!dir.exists()){
			dir.mkdir();
		}
		String dataName = dir.getName();
		vector = Vector.build(inputType);
		vector.setOutputType(outpuType);

		int blockId = 1;
		int partOfBlock = 0; // trim to size when this > 1;
	
		statWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_1.sta"));
		out = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_1.dat" ));
		int count = 0;
		for(String line = rawDataReader.readLine(); line != null ; line = rawDataReader.readLine()){
			vector.parseLine(line);
			count ++;
			statistic.stat(vector);
			boolean success = vector.appendToByteBuffer(buffer);
			if (success == false){
				for(;  buffer.position() < buffer.capacity(); ){
					buffer.putInt(0);
				}
				out.write(buffer.array());
				buffer.clear();
				partOfBlock += 1;
				if (partOfBlock >= 2){
					System.out.println(count);
					statWriter.write(statistic.toString()); //finish stat of this block
					statistic.clearStat();
					Closeables.close(out, true); //close old and open a new
					Closeables.close(statWriter, false);
					blockId += 1;
					out = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".dat" ));
					statWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".sta"));
					partOfBlock = 0;
					//re-filling
					vector.appendToByteBuffer(buffer);
				}
			}
		}
		System.out.println(count);
		//end of 
		out.write(buffer.array(), 0, buffer.position());
		statWriter.write(statistic.toString());
		
		Closeables.close(out, true);
		Closeables.close(rawDataReader, true);
		Closeables.close(statWriter, true);
	}
	
	
	/**
	 * convert fim data format
	 * @param outDir
	 * @throws IOException
	 */
	public void convertFID2FID(String outDir) throws IOException{
		generalConverting(outDir, VectorType.FIDONLY, VectorType.FIDONLY, new BasicStatistic());
	}

	public void convertLW2LW(String outDir) throws IOException{
		generalConverting(outDir, VectorType.LABEL_FID_WEIGHT, VectorType.LABEL_FID_WEIGHT, new LableWeightStatistic(new BasicStatistic()));
	}
		
}
