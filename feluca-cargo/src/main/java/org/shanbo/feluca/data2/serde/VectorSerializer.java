package org.shanbo.feluca.data2.serde;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Vector.VectorType;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.data.util.TextReader;

import com.google.common.io.CharSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

public class VectorSerializer {

	
	BufferedOutputStream dataOutput;
	BufferedWriter blockStatWriter;
	BufferedWriter globalStatWriter;
	BufferedReader rawDataReader;
	TextReader textReader;
	
	public VectorSerializer(String inFile) throws IOException{
		File input = new File(inFile);
		if (input.isFile()){
			rawDataReader = new BufferedReader(new FileReader(inFile));
		}else if (input.isDirectory()){
			File[] files = input.listFiles();
			
			ArrayList<CharSource> fileList= new ArrayList<CharSource>();
			for(File f: files){
				fileList.add(Files.asCharSource(f, Charset.defaultCharset()));
			}
			CharSource concat = CharSource.concat(fileList.iterator());
			rawDataReader = concat.openBufferedStream();
		}else{
			throw new FileNotFoundException("path : " + inFile + " not found");
		}
	}
	
	
	private void generalConverting(String outDir, VectorType inputType, VectorType outputType, DataStatistic globalStat) throws FileNotFoundException, IOException{
		File dir = new File(outDir);
		if (dir.isFile()){
			dir.delete();
		}
		if (!dir.exists()){
			dir.mkdir();
		}
		String dataName = dir.getName();
		globalStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + ".sta"));
		dataOutput= new BufferedOutputStream(new FileOutputStream(dir.getAbsolutePath() + "/" + dataName + ".dat"));
		MessagePack mPack = new MessagePack();
		Packer packer = mPack.createPacker(dataOutput);
		int count = 0;
		Vector vector = Vector.create(inputType);
		vector.setOutputType(outputType);
		for(String line = textReader.readLine(); line != null ; line = textReader.readLine()){
			
			boolean parse = vector.parseLine(line);
			if (parse == false){
				continue;
			}
			packer.write(true);
			vector.pack(packer);
			
			globalStat.stat(vector);
			count ++;
			if (count % 10000 == 0){
				System.out.println("!");
				
			}
		}
		packer.write(false); //end
		globalStatWriter.write(globalStat.toString());
		
		Closeables.close(dataOutput, true);
		Closeables.close(textReader, true);
		Closeables.close(blockStatWriter, true);
		Closeables.close(globalStatWriter, true);
		
	}

	
	public void convertLW2LW(String outFile) throws FileNotFoundException, IOException{
		textReader = new TextReader(rawDataReader);
		generalConverting(outFile, VectorType.LABEL_FID_WEIGHT, VectorType.LABEL_FID_WEIGHT, DataStatistic.createLWstat());
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		VectorSerializer vs = new VectorSerializer("E:/data/real-sim");
		vs.convertLW2LW("data/rrr");

	}

}
