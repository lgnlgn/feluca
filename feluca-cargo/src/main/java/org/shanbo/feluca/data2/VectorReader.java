package org.shanbo.feluca.data2;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data.BlockStatus;
import org.shanbo.feluca.data2.Vector.VectorType;

import com.google.common.io.Closeables;
import com.google.common.io.PatternFilenameFilter;

public class VectorReader implements Closeable{

//	ArrayList<InputStream> inputStreams;
	File[] listFiles;
	boolean hasNext = true;
	MessagePack msgpack ;
	Unpacker unpacker;
	
	Properties stat;
	VectorType vt;
	int blockIt = 0;
	public VectorReader(String dirName) throws IOException{
		File dir = new File(dirName);
		listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + "\\.\\d+\\.dat"));

		msgpack = new MessagePack();
		unpacker = msgpack.createUnpacker(new BufferedInputStream(new FileInputStream(listFiles[blockIt])));
		stat = BlockStatus.loadStatistic(dirName + "/" + dir.getName()  + ".sta"); 
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
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		VectorReader vr = new VectorReader("data/rrr");
		int count = 0;
		for(Vector v = vr.getNextVector(); v!= null; v = vr.getNextVector()){
			if (count < 10)
				System.out.println(v);
			count ++;
		}
		System.out.println(count);
	}

}
