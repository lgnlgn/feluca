package org.shanbo.feluca.data2;

import java.io.BufferedInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data2.Vector.VectorType;

import com.google.common.io.Closeables;
import com.google.common.io.PatternFilenameFilter;

/**
 * Reads blocks sequentially
 * @author lgn
 *
 */
public class SeqVectorReader implements VectorReader{

	File[] listFiles;
	boolean hasNext = true;
	MessagePack msgpack ;
	Unpacker unpacker;
	Random random  = new Random();
	Properties stat;
	VectorType vt;
	int blockIt = 0;
	
	File dir;
	
	public SeqVectorReader(String dirName) throws IOException{
		this(dirName, "\\.\\d+\\.dat"); // all ordinary
	}
	
	public File getDataDir(){
		return dir;
	}
	
	protected void shuffle(File[] files){
		for(int i = 0 ; i <  files.length; i++){
			int swap = random.nextInt(files.length);
			File tmp = files[swap];
			files[swap ] = files[i];
			files[i] = tmp;
		}
		for(int i = 0 ; i <  files.length; i++){
			int swap = random.nextInt(files.length);
			File tmp = files[swap];
			files[swap ] = files[i];
			files[i] = tmp;
		}
	}
	
	public SeqVectorReader(String dirName, String filterPattern) throws IOException{
		dir = new File(dirName);
		listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + filterPattern));
		if (listFiles == null || listFiles.length == 0){
			throw new RuntimeException("blocks not found!");
		}
		msgpack = new MessagePack();
		unpacker = msgpack.createUnpacker(new BufferedInputStream(new FileInputStream(listFiles[blockIt]),1024 * 1024 * 2));
		stat = new Properties();
		FileInputStream fis = new FileInputStream(dirName + "/" + dir.getName()  + ".sta"); 
		stat.load(fis);
		fis.close();
		vt = VectorType.valueOf(stat.getProperty("vectorType"));
	}
	
	public SeqVectorReader(String dirName, String filterPattern, boolean shuffle) throws IOException{
		dir = new File(dirName);
		listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + filterPattern));
		if (shuffle){
			shuffle(listFiles);
			
			System.out.println(Arrays.toString(listFiles));
		}else {
			//sort by blockid
		}
		if (listFiles == null || listFiles.length == 0){
			throw new RuntimeException("blocks not found!");
		}
		msgpack = new MessagePack();
		unpacker = msgpack.createUnpacker(new BufferedInputStream(new FileInputStream(listFiles[blockIt]),1024 * 1024 * 2));
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
//					System.out.println("!");
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
			
		}
	}
	
	
}
