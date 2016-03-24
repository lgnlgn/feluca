package org.shanbo.feluca.data2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.shanbo.feluca.data2.Vector.VectorType;

import com.google.common.io.PatternFilenameFilter;

/**
 * list of SeqVectorReader, combine vectors into one  at each Next() 
 * @author lgn
 *
 */
@Deprecated
public class MultiVectorReader implements VectorReader{
	List<SeqVectorReader> readers;
	List<Vector> currentVectors;
	int[] parts;
	VectorType vt;
	File dir;
	
	public MultiVectorReader(String dirName, int... parts) throws IOException{
		
		if (parts == null){ //all
			File dir = new File(dirName);
			File[] listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + "\\.v\\.\\d+\\.dat"));
			parts = new int[listFiles.length];
			for(int i = 0 ; i < listFiles.length; i++){
				String blockPart = listFiles[i].getName().split("\\.v\\.")[1];
				int blockId = Integer.parseInt(blockPart.substring(0, blockPart.indexOf(".dat")));
				parts[i] = blockId;
			}
		}else{
			this.parts = parts;
		}
		readers = new ArrayList<SeqVectorReader>(parts.length);
		currentVectors = new ArrayList<Vector>(parts.length);
		for(int i = 0 ; i < parts.length; i++){
			readers.add(new SeqVectorReader(dirName, "\\.v\\." + parts[i] +"\\.dat"));
		}
		vt = readers.get(0).getVectorType();
		dir = readers.get(0).getDataDir();
	}
	
	public File getDataDir(){
		return dir;
	}
	
	
	public int getBlocks(){
		return readers.size();
	}
	
	private Vector asOne(List<Vector> vectors){
		if (vectors == null || vectors.isEmpty()){
			return null;
		}
		Vector newVector = Vector.create(vt);
		for(Vector v : vectors){
			newVector.swallow(v);
		}
		return newVector;
	}
	
	public Vector getNextVector() throws IOException{
		getNextVectors();
		if (currentVectors.get(0) == null){
			return null;
		}else{
			return asOne(currentVectors);
		}
	}
	
	public List<Vector> getNextVectors() throws IOException{
		currentVectors.clear();
		for(SeqVectorReader vr : readers){
			currentVectors.add(vr.getNextVector());
		}
		return currentVectors;
	}
	
	
	public VectorType getVectorType(){
		return vt;
	}
	
	public Properties getDataStatistic(){
		return readers.get(0).getDataStatistic();
	}
	
	public void close(){
		for(SeqVectorReader reader : readers){
			reader.close();
		}
	}
	
}
