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
import org.shanbo.feluca.data2.Vector;
import org.shanbo.feluca.data2.VectorReader;

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
	
	public void doPartition(String dirName, int blocks) throws IOException{
		assert (isPowerOfTwo(blocks) == true);
		String dataName = new File(dirName).getName();
		String blockPathTemplate = new File(dirName).getAbsolutePath() + "/" + dataName + ".v.%d.dat";

		HashPartitioner partitioner = new HashPartitioner(blocks);
		VectorReader reader = new VectorReader(dirName);
		ArrayList<Packer> packers = new ArrayList<Packer>(blocks);
		MessagePack messagePack = new MessagePack();
		for(int i = 0 ; i < blocks;i++){ //output
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
	
	public void divideToDouble(String dataName){
		
	}
	
	
}
