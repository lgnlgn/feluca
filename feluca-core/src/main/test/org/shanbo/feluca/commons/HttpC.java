package org.shanbo.feluca.commons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.shanbo.feluca.node.http.HttpClientUtil;
import org.shanbo.feluca.util.DistributedRequester;
import org.shanbo.feluca.util.Strings;

public class HttpC {

	public static void main(String[] args) throws ClientProtocolException, IOException, InterruptedException, ExecutionException {
		HttpClient httpClient = HttpClientUtil.get().getHttpClient();
//		HttpResponse execute = httpClient.execute(new HttpGet("http://10.249.9.252:12120/jk"));
//		String httpclientResponseToString = Strings.httpclientResponseToString(execute);
//		System.out.println(httpclientResponseToString);
		ArrayList<String> address = new ArrayList<String>();
		address.add("10.249.9.252:12020");
		address.add("10.249.9.252:12320");
		List<String> broadcast = DistributedRequester.get().broadcast("/a", "",address);
		System.out.println(broadcast);
	}

}
