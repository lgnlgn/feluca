package org.shanbo.feluca.data2.serde;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.shanbo.feluca.data.BlockStatus;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.Vector.VectorType;

import com.google.common.io.ByteSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.PatternFilenameFilter;

public class VectorReader implements Closeable{

	InputStream in;
	boolean hasNext = true;
	MessagePack msgpack ;
	Unpacker unpacker;
	
	Properties stat;
	VectorType vt;
	
	public VectorReader(String dirName) throws IOException{
		File dir = new File(dirName);
//		File[] listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + "_\\d+\\.dat"));
//		ArrayList<ByteSource> inList = new ArrayList<ByteSource>();
//		for(File f: listFiles){
//			inList.add(Files.asByteSource(f));
//		}
//		ByteSource concat = ByteSource.concat(inList.iterator());
//		in = concat.openBufferedStream();
		in = new BufferedInputStream(new FileInputStream(dirName + "/" + dir.getName() + ".dat"));
		msgpack = new MessagePack();
		unpacker = msgpack.createUnpacker(in);
		stat = BlockStatus.loadStatistic(dirName + "/" + dir.getName()  + ".sta"); 
		vt = VectorType.valueOf(stat.getProperty("vectorType"));
	}
	
	
	public Vector getNextVector() throws IOException{
		if (hasNext){
			Boolean read = unpacker.readBoolean();
			if (read == true){
				return Vector.create(vt, unpacker);
			}else{
				hasNext = false;
				Closeables.close(unpacker, true);
				Closeables.close(in, true);
				return null;
			}
		}else{
			return null;
		}
	}
	
	public void close(){
		try {
			Closeables.close(unpacker, true);
			Closeables.close(in, true);
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
