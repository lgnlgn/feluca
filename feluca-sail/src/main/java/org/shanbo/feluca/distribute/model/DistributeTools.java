package org.shanbo.feluca.distribute.model;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

/**
 * no need to set parameters for 
 * @author lgn
 *
 */
public class DistributeTools {
	final HttpClient client ;
	BytesPark[] caches ;
	String[] address;
	
	class RequstCallable implements Callable<Void>{

		String requestMark;
		BytesPark bytesPark;
		public RequstCallable(String requestMark, BytesPark bytesPark){
			this.bytesPark = bytesPark;
			this.requestMark = requestMark;
		}

		public Void call() throws Exception {
			//TODO
			HttpPost post = new HttpPost();
			post.setEntity( new ByteArrayEntity(bytesPark.getArray(), 0, bytesPark.arraySize()));
			HttpResponse response = client.execute(post);
			final StatusLine statusLine = response.getStatusLine();
			final HttpEntity entity = response.getEntity();
			if (statusLine.getStatusCode() >= 300) {
				EntityUtils.consume(entity);
				throw new HttpResponseException(statusLine.getStatusCode(),
						statusLine.getReasonPhrase());
			}
			InputStream in = entity.getContent();
			bytesPark.fill(in);
			in.close();
			return null;
		}

	}


	public DistributeTools(GlobalConfig conf){
		client = HttpClientBuilder.create().useSystemProperties().build();
		caches = new BytesPark[conf.nodes()];
		address =new String[conf.nodes()];
		for(int i = 0 ; i < conf.nodes(); i++){
			caches[i] = new BytesPark();
			address[i]= conf.getConfigByPart(i).getString("address");
		}
	}


	public BytesPark getByteArray(int part){
		return caches[part];
	}


	/**
	 * TODO concurrent RPC
	 * @param mark
	 * @param body
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void fetchModelBack() throws InterruptedException, ExecutionException{
		request("/fetch");
	}
	
	/**
	 * send update request to remote 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void updateModel() throws InterruptedException, ExecutionException{
		request("/update");
	}
	
	private void request(String path) throws InterruptedException, ExecutionException{
		List<Callable<Void>> messageSend = new ArrayList<Callable<Void>>(caches.length);
		for(int i = 0 ;i < caches.length;i++){
			messageSend.add(new RequstCallable(path, caches[i]));
		}
		ConcurrentExecutor.execute(messageSend);
	}
}
