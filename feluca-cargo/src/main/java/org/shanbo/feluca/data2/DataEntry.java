package org.shanbo.feluca.data2;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.shanbo.feluca.data2.Vector.VectorType;

/**
 * sugar for VectorReader
 * TODO add a queue for producer/consumer mode
 * @author lgn
 *
 */
public class DataEntry implements Closeable{
	
	VectorReader reader;
	String dataName;
	String pattern ;
	BlockingQueue<Vector> queue;
	Thread reading ;
	volatile boolean finished = false;
	public DataEntry(String dataName) throws IOException{
		this(dataName, "\\.\\d+\\.dat");
	}
	
	public DataEntry(String dataName, String pattern) throws IOException{
		this.dataName = dataName;
		this.pattern = pattern;
		reader = new SeqVectorReader(dataName, pattern);
		queue = new LinkedBlockingQueue<Vector>(1000);
	}
	
	public void reOpen() throws Exception{
		close();
		reader = new SeqVectorReader(dataName, pattern);
//		finished = false;
//		reading = new Thread(new Runnable() {
//			public void run() {
//				try {
//					fill();
//				} catch (Exception e) {
//					throw new RuntimeException("filling queue error!", e);
//				}
//			}
//		});
//		reading.setDaemon(true);
//		reading.start();
	}
	
	private void fill() throws IOException, InterruptedException{
		
		for(Vector v = reader.getNextVector(); v!= null;){
			boolean inserted = queue.offer(v);
			if (inserted){
				v = reader.getNextVector();
			}else{
				Thread.sleep(5);
				continue;
			}
		}
		finished = true;
	}
	
	public Vector getNextVector() throws Exception{
//		Vector v = queue.poll();
//		if (v == null){
//			if (finished == true){
//				return null;
//			}else{
//				Thread.sleep(10);
//				return getNextVector();
//			}
//		}else{
//			return v;
//		}
		return reader.getNextVector();
	}
	
	public VectorType getVectorType(){
		return reader.getVectorType();
	}
	
	public Properties getDataStatistic(){
		return reader.getDataStatistic();
	}
	
	public void close() throws IOException{
		queue.clear();
		if (reader!=null)
			reader.close();
	}
	
	public static DataEntry createDataEntry(String dataName, boolean inRam) throws IOException{
		if (inRam){
			return new RAMDataEntry(dataName);
		}else {
			return new DataEntry(dataName);
		}
	}
	
	public static DataEntry createDataEntry(String dataName, String pattern, boolean inRam) throws IOException{
		if (inRam){
			return new RAMDataEntry(dataName, pattern);
		}else {
			return new DataEntry(dataName, pattern );
		}
	}
	
	
	public static class RAMDataEntry extends DataEntry{

		Vector[] vectors;
		int idx = 0;
		
		public RAMDataEntry(String dataName) throws IOException{
			super(dataName);
		}
		
		public RAMDataEntry(String dataName, String pattern) throws IOException{
			super(dataName, pattern);
		}
		
		public Vector getVectorByIndex(int index){
			return vectors[index];
		}
		
		public Vector getNextVector(){
			return vectors[idx++];
		}
		
		public void reOpen() throws Exception{
			if (vectors == null){
				super.reOpen();
				vectors = new Vector[
				                     Integer.parseInt(reader.getDataStatistic().getProperty(DataStatistic.NUM_VECTORS)) + 1];
				idx = 0;
				for(Vector v = super.getNextVector(); v!= null; v = super.getNextVector()){
					vectors[idx++] = v;
				}
				super.close();
			}
			idx = 0;
		}
		
	}
	public static void main(String[] args) throws Exception {
		DataEntry de =  DataEntry.createDataEntry("data/real-sim", false);
		System.out.println("!!!");
		de.reOpen();
		int count = 0;
		for(Vector v = de.getNextVector(); v!= null ; v = de.getNextVector()){
			count +=1;
		}
		System.out.println(count);
	}
}
