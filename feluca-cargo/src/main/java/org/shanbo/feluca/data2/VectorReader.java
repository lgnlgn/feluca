package org.shanbo.feluca.data2;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data2.Vector.VectorType;

import com.google.common.io.Closeables;
import com.google.common.io.PatternFilenameFilter;

public class VectorReader implements Closeable{

	File[] listFiles;
	boolean hasNext = true;
	MessagePack msgpack ;
	Unpacker unpacker;
	
	Properties stat;
	VectorType vt;
	int blockIt = 0;
	public VectorReader(String dirName) throws IOException{
		this(dirName, "\\.\\d+\\.dat"); // all ordinary
	}
	
	public VectorReader(String dirName, String filterPattern) throws IOException{
		File dir = new File(dirName);
		listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + filterPattern));

		msgpack = new MessagePack();
		unpacker = msgpack.createUnpacker(new BufferedInputStream(new FileInputStream(listFiles[blockIt])));
		stat = new Properties();
		FileInputStream fis = new FileInputStream(dirName + "/" + dir.getName()  + ".sta"); 
		stat.load(fis);
		fis.close();
		vt = VectorType.valueOf(stat.getProperty("vectorType"));
	}
	
	public VectorType getVectorType(){
		return vt;
	}
	
	public Properties getDataStatistic(){
		return stat;
	}
	
	
	public Vector getNextVector() throws IOException{
		if (hasNext){
			Boolean read = unpacker.readBoolean();
			if (read == true){
				return Vector.create(vt, unpacker);
			}else{
				Closeables.close(unpacker, true);
				blockIt ++;
				if (blockIt >= listFiles.length){
					hasNext = false;
					return null;
				}else{
					System.out.println("!");
					unpacker = msgpack.createUnpacker(new BufferedInputStream(new FileInputStream(listFiles[blockIt])));
					return getNextVector();
				}
			}
		}else{
			return null;
		}
	}
	
	public void close(){
		try {
			Closeables.close(unpacker, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
}
