package org.shanbo.feluca.data;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.shanbo.feluca.common.FelucaException;
import org.shanbo.feluca.data.Vector.VectorType;

import com.google.common.io.Closeables;

public class DataConverter {
	ByteBuffer buffer;
	
	Vector vector;
	
	BufferedOutputStream out;
	
	BufferedReader reader;
	
	public DataConverter(String inFile) throws FileNotFoundException{
		reader = new BufferedReader(new FileReader(inFile));
	}
	
	/**
	 * convert fimi data format
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
		for(String line = reader.readLine(); line != null ; line = reader.readLine()){
			vector.parseLine(line);
			boolean success = vector.appendToByteBuffer(buffer);
			if (success == false){
				if (partOfBlock >= 2){
					Closeables.close(out, true); //close old and open a new
					blockId += 1;
					out = new BufferedOutputStream(new FileOutputStream(
							dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".dat" ));
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
		//end of 
		if (partOfBlock >= 2){
			Closeables.close(out, true); //close old and open a new
			blockId += 1;
			partOfBlock = 0;
			out = new BufferedOutputStream(new FileOutputStream(
					dir.getAbsolutePath() + "/" + dataName + "_" + blockId + ".dat" ));
		}
		System.out.println(buffer.position());
		System.out.println(buffer.arrayOffset());
		System.out.println(buffer.capacity());
		out.write(buffer.array(), 0, buffer.position());
		Closeables.close(out, false);
	}
	
}
