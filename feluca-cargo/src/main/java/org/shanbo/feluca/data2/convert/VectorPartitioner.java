package org.shanbo.feluca.data2.convert;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.shanbo.feluca.data2.HashPartitioner;
import org.shanbo.feluca.data2.SeqVectorReader;
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.VectorReader;

import com.google.common.io.PatternFilenameFilter;

/**
 * 
 * @author lgn
 *
 */
public class VectorPartitioner {

	public boolean isPowerOfTwo(int number){
		int n = number;
		while(n > 1){
			int mod = number %2;
			if (mod > 0){
				return false;
			}
			n = number >>> 1;
		}
		return true;
	}


	void doPartition(VectorReader reader, int shards, String suffix) throws IOException{
		assert (isPowerOfTwo(shards) == true);
		File[] dats = reader.getDataDir().listFiles(new PatternFilenameFilter(".*\\.v\\.\\d+\\.dat"));
		for(File dat : dats){
			System.out.print(dat.getName() + ";");
			dat.delete();
		}
		String dataName = reader.getDataDir().getName();
		HashPartitioner partitioner = new HashPartitioner(shards);
		String blockPathTemplate = reader.getDataDir().getAbsolutePath() + "/" + dataName + ".v.%d.dat" + suffix;

		ArrayList<Packer> packers = new ArrayList<Packer>(shards);
		MessagePack messagePack = new MessagePack();
		for(int i = 0 ; i < shards;i++){ //output
			packers.add(messagePack.createPacker(
					new BufferedOutputStream(new FileOutputStream(String.format(blockPathTemplate, i)), 1024 * 1024 * 2)));
		}

		int count = 0;
		for(Vector v = reader.getNextVector(); v!= null; v = reader.getNextVector()){
			List<Vector> divided = v.divideByFeature(partitioner); 
			for(int i = 0 ; i < divided.size(); i++){
				packers.get(i).write(true);
				divided.get(i).pack(packers.get(i));
			}
			count ++;
			if (count % 2000 == 0){
				for(int i = 0 ; i < packers.size(); i++){
					packers.get(i).flush();
				}
				System.out.print("*");
			}
		}
		for(int i = 0 ; i < packers.size(); i++){
			packers.get(i).write(false).close();
		}
	}

	public void doPartition(String dirName, int shards) throws IOException{
		SeqVectorReader vr = new SeqVectorReader(dirName);
		doPartition(vr, shards, "");
	}

//	/**
//	 * repartition data ; example : [0,1] -> [0,1,2,3]
//	 * @param dirName
//	 * @throws IOException
//	 */
//	public void divideToDouble(String dirName) throws IOException{
//		MultiVectorReader multiVectorReader = new MultiVectorReader(dirName, null);
//		doPartition(multiVectorReader, multiVectorReader.getBlocks() * 2, ".new");
//		for(int i = 0 ; i < )
//	}


}
