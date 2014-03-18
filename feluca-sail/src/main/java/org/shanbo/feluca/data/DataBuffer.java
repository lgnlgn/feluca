package org.shanbo.feluca.data;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class DataBuffer implements Runnable{
	final static int CACHE_SIZE = 32 * 1024*1024;
	
	List<BlockStatus> blocks; 

	int partOfBlock;
	
	byte[] readingCache ;
	byte[] writingCache;
	
	FileInputStream in;
	int currentBlock;
	
	volatile boolean reading;  //control by reader, the writer need to monitor it
	volatile boolean writingfinished; // flag for fetching process
	volatile boolean finished; //  control by writer, the reader need to monitor it
	boolean written ;

	public BlockStatus getCurrentBlockStatus(){
		return blocks.get(currentBlock);
	}
	
	public DataBuffer(String name){
		//TODO
		readingCache = new byte[CACHE_SIZE];
		writingCache = new byte[CACHE_SIZE];
	}
	
	/**
	 * call by outside
	 * @return
	 */
	synchronized byte[] getByteArrayRef(){
		while(true){ //
			if (written == true){
				return readingCache;
			}else{
				//wait until
				if (finished ){
					return null;
				}else{
					;
				}
			}
		}
	}
	
	/**
	 * call by outside
	 */
	synchronized void releaseByteArrayRef(){
		reading = false;
	}
	
	/**
	 * only call by 
	 */
	private void switchCache(){
		//TODO
		byte[] tmp = readingCache;
		readingCache = writingCache;
		writingCache = tmp;
		reading = true;      // close for the writer
		written = false; 
		if (writingfinished){
			finished = true;
		}
 	}
	
	
	private void fillCache() throws IOException{
		in.read(writingCache, 0, getCurrentBlockStatus().offsets[partOfBlock++]);
		if (partOfBlock >= getCurrentBlockStatus().offsets.length){
			in.close();
			partOfBlock = 0;
			//move to next block
			currentBlock +=1;
			if (currentBlock < blocks.size()){
				in = new FileInputStream(getCurrentBlockStatus().blockPath);
			}else{
				finished = true;
			}
		}
		written = true;
		
	}
	

	
	//TODO
	public void reset(){
		
	}

	public void run() {
		while(!finished){
			if (reading && written){ // need to waiting for reader
				;
			}else if (!reading && written){ //reading finish, reader waits for switch, 
				this.switchCache();
			}else{
				// read some vectors from file to ram each loop, do not switch immediately.  
				try{
					this.fillCache(); 
				}catch (IOException e) {
					throw new RuntimeException("IO exception!!!!!!!");
				}
			}
		}
	}
	
}
