package org.shanbo.feluca.node.http;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.shanbo.feluca.util.DistributedRequester;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public class HttpRequester extends DistributedRequester{

	public static class HttpClientCallable implements Callable<String>{
		
		String url ;
		byte[] content;
		public HttpClientCallable(String url, byte[] content){
			this.url = url;
		}
		public String call() throws Exception {
			ByteArrayEntity bae = new  ByteArrayEntity(content);
			HttpPost post = new HttpPost(url);
			post.setEntity(bae);
			return HttpClientUtil.get().getHttpClient().execute(post, new BasicResponseHandler());
		}
		
	}
	
	public List<String> broadcast(String action, Object content, List<String> audiences) throws InterruptedException, ExecutionException {
		if (audiences == null)
			return Collections.emptyList();
		byte[] bytes = content.toString().getBytes();
		List<Callable<String>> toSend = new ArrayList<Callable<String>>();
		for(String address : audiences){
			toSend.add(new HttpClientCallable("http://" + address + "/" + StringUtils.strip(action, "/"), bytes));
		}
		return request(toSend);
	}

	@Override
	public List<String> request(List<Callable<String>> callables)
			throws InterruptedException, ExecutionException {
		return ConcurrentExecutor.execute(callables);
	}
	
	
	
//	public ChannelFuture asynDoGet(ClientBootstrap client, String hostPort, String uri){
//		String[] hp = hostPort.split(":");
//		return asynDoGet(client, hp[0], new Integer(hp[1]), uri);
//	}
//
//	public ChannelFuture asynDoGet(ClientBootstrap client, SocketAddress address, String uri){
//		ChannelFuture future = client.connect(address);
//		Channel channel = future.awaitUninterruptibly().getChannel();
//		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
//				HttpMethod.GET, uri);
//		channel.write(request);
//		return future;
//	}
//	
//	public ChannelFuture asynDoGet(ClientBootstrap client, String host, int port, String uri){
//		SocketAddress remoteAddress = new InetSocketAddress(host, port);
//		return asynDoGet(client, remoteAddress, uri);
//	}
//	
//	public ChannelFuture asynDoPost(ClientBootstrap client, String host, int port, String uri, byte[] content){
//		SocketAddress remoteAddress = new InetSocketAddress(host, port);
//		return asynDoPost(client, remoteAddress, uri, content);
//	}
//	
//	public ChannelFuture asynDoPost(ClientBootstrap client, String hostPort, String uri, byte[] content){
//		String[] hp = hostPort.split(":");
//		return asynDoPost(client, hp[0], new Integer(hp[1]), uri, content);
//	}
//	
//	public ChannelFuture asynDoPost(ClientBootstrap client, SocketAddress address, String uri, byte[] content){
//		ChannelFuture future = client.connect(address);
//		Channel channel = future.awaitUninterruptibly().getChannel();
//		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
//				HttpMethod.POST, uri);
//		request.setContent(ChannelBuffers.copiedBuffer(content));
//		channel.write(request);
//		return future;
//	}
	
}
