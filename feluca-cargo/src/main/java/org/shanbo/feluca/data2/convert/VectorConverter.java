package org.shanbo.feluca.data2.convert;

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
import org.shanbo.feluca.data2.DataSetInfo;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Vector.VectorType;
import org.shanbo.feluca.data2.util.TextReader;
import org.shanbo.feluca.vectors.LabelVector;

import com.google.common.io.CharSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

public class VectorConverter {
	
	static int OUTPUT_BUFFER_SIZE = 8 * 1024 * 1024;
	
	BufferedWriter globalStatWriter;
	BufferedReader rawDataReader;
	TextReader textReader;
	String fileName;
	public VectorConverter(String inFile) throws IOException{
		File input = new File(inFile);
		fileName = input.getName();
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
	
	private void generalConverting(String outDir, Vector vector) throws FileNotFoundException, IOException{
		
		
		File dir = new File(outDir);
		if (dir.isFile()){
			dir.delete();
		}
		if (!dir.exists()){
			dir.mkdir();
		}
		String blockPathTemplate = dir.getAbsolutePath() + "/" + fileName + ".ser";
		int blockSize = 0;
		globalStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + fileName + ".sta"));
		MessagePack mPack = new MessagePack();
		Packer packer = mPack.createPacker( new BufferedOutputStream(new FileOutputStream(blockPathTemplate),OUTPUT_BUFFER_SIZE ));
		int count = 0;
		DataSetInfo dataSetInfo = new DataSetInfo();

		for(String line = textReader.readLine(); line != null ; line = textReader.readLine()){
			
			boolean parse = vector.parseLine(line);
			if (parse == false){
				continue;
			}
			packer.write(true);
			vector.pack(packer);
			
			dataSetInfo.doStat(vector);
			count ++;
			if (count % 10000 == 0){
//				System.out.println("!");
			}
			blockSize += vector.getSpaceCost() ;
		}
		packer.write(false).close();
		globalStatWriter.write(DataSetInfo.prop2String(dataSetInfo.getStatInfo()));
		
		Closeables.close(textReader, true);
		Closeables.close(globalStatWriter, true);
		
	}
	
	public void convertLW2LW(String outFile) throws  IOException{
		textReader = new TextReader(rawDataReader);
		generalConverting(outFile, new LabelVector());
	}
}
