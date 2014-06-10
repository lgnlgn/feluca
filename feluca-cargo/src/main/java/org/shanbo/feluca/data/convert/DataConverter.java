package org.shanbo.feluca.data.convert;

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

import org.shanbo.feluca.data.Vector;
import org.shanbo.feluca.data.Vector.VectorType;
import org.shanbo.feluca.data.convert.DataStatistic.BasicStatistic;
import org.shanbo.feluca.data.convert.DataStatistic.LableWeightStatistic;

import com.google.common.io.Closeables;

public class DataConverter {
	ByteBuffer buffer;
	
	Vector vector;
	
	BufferedOutputStream dataOutput;
	BufferedWriter blockStatWriter;
	BufferedWriter globalStatWriter;
	BufferedReader rawDataReader;
	
	public DataConverter(String inFile) throws FileNotFoundException{
		rawDataReader = new BufferedReader(new FileReader(inFile));
		buffer = ByteBuffer.wrap(new byte[32 * 1024 * 1024]);
	}
	
	private void generalConverting(String outDir, VectorType inputType, VectorType outpuType, DataStatistic blockStat, DataStatistic globalStat) throws FileNotFoundException, IOException{
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
	
		
		globalStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + ".sta"));
		
		blockStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_1.sta"));
		dataOutput = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_1.dat" ));
		int count = 0;
		for(String line = rawDataReader.readLine(); line != null ; line = rawDataReader.readLine()){
			boolean parseOk = vector.parseLine(line);
			if (parseOk == false){
				continue;
			}
			globalStat.stat(vector);
			count ++;
			boolean success = vector.appendToByteBuffer(buffer);
			if (success == false){
				for(;  buffer.position() < buffer.capacity(); ){
					buffer.putInt(0);//fill 0s
				}
				dataOutput.write(buffer.array());
				buffer.clear();
				partOfBlock += 1;
				if (partOfBlock >= 2){
					System.out.println(count);
					blockStatWriter.write(blockStat.toString()); //finish stat of this block
					blockStat.clearStat();
					Closeables.close(dataOutput, true); //close old and open a new
					Closeables.close(blockStatWriter, false);
					blockId += 1;
					dataOutput = new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".dat" ));
					blockStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".sta"));
					partOfBlock = 0;
				}
				//refill to either 'new block' or '2nd part' 
				vector.appendToByteBuffer(buffer);
				blockStat.stat(vector);
			}else{
				blockStat.stat(vector);
			}
			
		}
		System.out.println(count);
		//end of 
		dataOutput.write(buffer.array(), 0, buffer.position());
		blockStatWriter.write(blockStat.toString());
		globalStatWriter.write(globalStat.toString());
		
		Closeables.close(dataOutput, true);
		Closeables.close(rawDataReader, true);
		Closeables.close(blockStatWriter, true);
		Closeables.close(globalStatWriter, true);
	}
	
	
	/**
	 * convert fim data format
	 * @param outDir
	 * @throws IOException
	 */
	public void convertFID2FID(String outDir) throws IOException{
		generalConverting(outDir, VectorType.FIDONLY, VectorType.FIDONLY, 
				new BasicStatistic(),new BasicStatistic());
	}

	public void convertLW2LW(String outDir) throws IOException{
		generalConverting(outDir, VectorType.LABEL_FID_WEIGHT, VectorType.LABEL_FID_WEIGHT, 
				new LableWeightStatistic(new BasicStatistic()),  new LableWeightStatistic(new BasicStatistic()));
	}
		
}
