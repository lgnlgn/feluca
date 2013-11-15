package org.shanbo.feluca.util;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;


public class HttpClientUtil {
	private static HttpClientUtil http = new HttpClientUtil();
	private HttpClient httpClient ;
	
	private HttpClientUtil(){
	    ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager();
	    httpClient = new DefaultHttpClient(mgr);	
	}
	
	public static HttpClientUtil get(){
		return http;
	}
	
	/**
	 * fetch httpClient for your usage,
	 * you can also use {@link #doGet(String)} or {@link #doPost(String, String)} in common 
	 * @return
	 */
	public HttpClient getHttpClient(){
		return httpClient;
	}
	
	public String doGet(String url) throws ClientProtocolException, IOException{
		HttpGet get = new HttpGet(url);
		return httpClient.execute(get, new BasicResponseHandler());
	}
}
