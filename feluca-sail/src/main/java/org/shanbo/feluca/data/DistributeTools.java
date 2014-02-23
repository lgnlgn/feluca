package org.shanbo.feluca.data;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

public class DistributeTools {
	final HttpClient client ;
	BytesPark[] caches ;

	class RequstCallable implements Callable<Object>{

		String requestMark;
		BytesPark bytesPark;
		public RequstCallable(String requestMark, BytesPark bytesPark){
			this.bytesPark = bytesPark;
			this.requestMark = requestMark;
		}

		public Object call() throws Exception {
			HttpPost post = new HttpPost();
			post.setEntity( new ByteArrayEntity(bytesPark.getBytes(), 0, bytesPark.arraySize()));
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


	public DistributeTools(BytesPark[] caches){
		client = HttpClientBuilder.create().useSystemProperties().build();
		this.caches = caches;
	}


	public BytesPark getByteArray(int part){
		return caches[part];
	}


	/**
	 * TODO concurrent RPC
	 * @param mark
	 * @param body
	 * @return
	 */
	public void request(String requestMark){

	}
}
