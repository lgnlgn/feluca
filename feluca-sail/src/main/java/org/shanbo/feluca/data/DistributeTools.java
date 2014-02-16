package org.shanbo.feluca.data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

public class DistributeTools {
	HttpClient client = new DefaultHttpClient();
	BytesPark caches ;
	
	class RequstCallable implements Callable<Object>{

		byte[] arrayPack ;
		String requestMark;
		public RequstCallable(String requestMark, byte[] byteArray){
			this.arrayPack = byteArray;
			this.requestMark = requestMark;
		}
		
		public Object call() throws Exception {
			request(requestMark, arrayPack);
			return null;
		}
		
	}
	
	
	public DistributeTools(int blocks){
		caches = new BytesPark(blocks);
	}
	
	
	public BytesPark getByteArray(){
		return caches;
	}

	
	/**
	 * TODO concurrent RPC
	 * @param mark
	 * @param body
	 * @return
	 */
	public void request(String requestMark, byte[] toFill){
		
	}
}
