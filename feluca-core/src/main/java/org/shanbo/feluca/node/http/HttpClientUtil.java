package org.shanbo.feluca.node.http;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

public class HttpClientUtil {
	public static final String PROP_SO_TIMEOUT = "socketTimeout";
	// connection timeout measures in ms, closes a socket if connection
	public static final String PROP_CONNECTION_TIMEOUT = "connTimeout";
	private static HttpClientUtil instance = new HttpClientUtil();

	private HttpClient client ;

	private HttpClientUtil(){
		ThreadSafeClientConnManager mgr = new ThreadSafeClientConnManager();
		mgr.setDefaultMaxPerRoute(256);
		
		client = new DefaultHttpClient(mgr);
		client.getParams().setIntParameter(PROP_SO_TIMEOUT, 5000);
		client.getParams().setIntParameter(PROP_CONNECTION_TIMEOUT, 3000);
	}

	public static HttpClientUtil get(){
		return instance;
	}

	/**
	 * 
	 * @param encodedURL
	 * @param connTimeOut
	 * @param soTimeOut
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public String doGet(String encodedURL, long connTimeOut, long soTimeOut) throws ClientProtocolException, IOException{
		HttpGet get = new HttpGet(encodedURL);
		get.getParams().setLongParameter(PROP_SO_TIMEOUT, soTimeOut);
		get.getParams().setLongParameter(PROP_CONNECTION_TIMEOUT, connTimeOut);
		return client.execute(get, new BasicResponseHandler());
	}

	public String doGet(String encodedURL) throws ClientProtocolException, IOException{
		return doGet(encodedURL, 3000, 5000);
	}
	
	
	public HttpClient getHttpClient(){
		return client; 
	}

}
