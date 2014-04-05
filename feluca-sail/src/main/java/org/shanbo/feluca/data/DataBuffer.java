package org.shanbo.feluca.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.PatternFilenameFilter;


public class DataBuffer implements Runnable{
	final static int CACHE_SIZE = 32 * 1024*1024;

	List<BlockStatus> blocks; 
	long dataLength = -1;
	int offsetIndex; // 

	byte[] readingCache ; 
	int readingLength;
	byte[] writingCache;
	int readLength;
	FileInputStream in;

	int currentBlock; //block id
	int currentAccBlockSize;

	volatile boolean reading;  //control by reader, the writer need to monitor it
	volatile boolean writingfinished; // flag for fetching process
	volatile boolean finished; //  control by writer, the reader need to monitor it
	boolean written ;


	public BlockStatus getCurrentBlockStatus(){
		return blocks.get(currentBlock);
	}

	public int getCurrentBytesLength(){
		return readingLength;
	}

	public synchronized long getDataLength(){
		return dataLength;
	}

	public DataBuffer(String dirName){
		readingCache = new byte[CACHE_SIZE];
		writingCache = new byte[CACHE_SIZE];
		File dir = new File(dirName);
		File[] listFiles = dir.listFiles(new PatternFilenameFilter(dir.getName() + "_\\d+\\.dat"));
		blocks = new ArrayList<BlockStatus>(listFiles.length);
		for(File dat : listFiles){
			blocks.add(new BlockStatus(dat.getAbsolutePath()
					.substring(0,dat.getAbsolutePath().length() - 4)));
		}

		if (dataLength == -1){
			dataLength = 0;
			for(int i = 0 ; i < blocks.size(); i++){
				dataLength += blocks.get(i).getBlockSize();
			}
		}
	}

	/**
	 * call by outside, current 
	 * @return
	 */
	synchronized byte[] getByteArrayRef(){
		while(true){ //
			if (reading == true){
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
		byte[] tmpCache = readingCache;
		readingCache = writingCache;
		writingCache = tmpCache;

		readingLength = readLength;
		readLength = 0;

		reading = true;      // close for the writer
		written = false; 
		if (writingfinished){
			finished = true;
		}
	}


	private void fillCache() throws IOException{
		if (in == null){
			in = new FileInputStream(getCurrentBlockStatus().block);
			currentAccBlockSize = getCurrentBlockStatus().getBlockSize();
		}
		System.out.print("-------from file");
		readLength = in.read(writingCache, 0, Math.min(writingCache.length,currentAccBlockSize));
		currentAccBlockSize -= readLength;
		if (currentAccBlockSize == 0){
			in.close();
			//move to next block
			currentBlock +=1;
			if (currentBlock < blocks.size()){
				in = new FileInputStream(getCurrentBlockStatus().block);
				currentAccBlockSize = getCurrentBlockStatus().getBlockSize();
			}else{
				writingfinished = true;
			}
		}
		System.out.print("-------finished:");
		written = true;

	}

	public void start() throws IOException{
		finished = false;
		written = false;
		writingfinished = false;
		fillCache();
		switchCache();
		Thread t = new Thread(this);
		t.setDaemon(true);
		t.start();
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
		System.out.println("thread finished!");
	}

	public static void main(String[] args) throws IOException {
		DataBuffer db = new DataBuffer("data/aaa");
		System.out.println();
		long t1 = System.nanoTime();
		db.start();
		//		System.out.println(db.getDataLength() + "\n");

		for(byte[] ref = db.getByteArrayRef(); ref != null;ref = db.getByteArrayRef() ){
			int currentBytesLength = db.getCurrentBytesLength();
			System.out.println(currentBytesLength);
			db.releaseByteArrayRef();
		}
		long t2 = System.nanoTime();
		System.out.println("ms:" + (t2-t1)/1000000);
	}
}
