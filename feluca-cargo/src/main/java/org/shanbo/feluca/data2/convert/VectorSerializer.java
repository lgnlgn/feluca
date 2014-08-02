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
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Tuple.AlignColumn;
import org.shanbo.feluca.data2.Tuple.TupleType;
import org.shanbo.feluca.data2.Vector.VectorType;
import org.shanbo.feluca.data2.DataStatistic;
import org.shanbo.feluca.data2.util.TextReader;

import com.google.common.io.CharSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

public class VectorSerializer {
	final static int DATA_SIZE_PER_BLOCK = 64 * 1024 * 1024; //6
	final static int OUTPUT_BUFFER_SIZE = 8 * 1024 * 1024;
	
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
		String blockPathTemplate = dir.getAbsolutePath() + "/" + dataName + ".%d.dat";
		int blockId = 1;
		int blockSize = 0;
		globalStatWriter = new BufferedWriter(new FileWriter(dir.getAbsolutePath() + "/" + dataName + ".sta"));
		MessagePack mPack = new MessagePack();
		Packer packer = mPack.createPacker( new BufferedOutputStream(new FileOutputStream(String.format(blockPathTemplate, blockId)),OUTPUT_BUFFER_SIZE ));
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
			blockSize += vector.getSpaceCost() ;
			if (blockSize > DATA_SIZE_PER_BLOCK){
				packer.write(false).close();
				blockId += 1;
				blockSize = 0;
				packer = mPack.createPacker( new BufferedOutputStream(new FileOutputStream(String.format(blockPathTemplate, blockId)),OUTPUT_BUFFER_SIZE));
			}
		}
		packer.write(false).close();
		globalStatWriter.write(globalStat.toString());
		
		Closeables.close(textReader, true);
		Closeables.close(globalStatWriter, true);
		
	}

	
	public void convertLW2LW(String outFile) throws  IOException{
		textReader = new TextReader(rawDataReader);
		generalConverting(outFile, VectorType.LABEL_FID_WEIGHT, VectorType.LABEL_FID_WEIGHT, DataStatistic.createLWstat());
	}
	
	public void convertVW2VW(String outFile) throws  IOException{
		textReader = new TextReader(rawDataReader);
		generalConverting(outFile, VectorType.LABEL_FID_WEIGHT, VectorType.LABEL_FID_WEIGHT, DataStatistic.createVWstat());
	}
	
	public void convertTuple2VID(String outFile) throws IOException{
		textReader = new TextReader(rawDataReader, TupleType.WEIGHT_TYPE, AlignColumn.FIRST);
		generalConverting(outFile, VectorType.VID_FID_WEIGHT, VectorType.VID_FID_WEIGHT, DataStatistic.createVWstat());
	}

}
