package org.shanbo.feluca.common.http;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;

public class HttpRequester {
	public ChannelFuture asynDoGet(ClientBootstrap client, String hostPort, String uri){
		String[] hp = hostPort.split(":");
		return asynDoGet(client, hp[0], new Integer(hp[1]), uri);
	}

	public ChannelFuture asynDoGet(ClientBootstrap client, SocketAddress address, String uri){
		ChannelFuture future = client.connect(address);
		Channel channel = future.awaitUninterruptibly().getChannel();
		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
				HttpMethod.GET, uri);
		channel.write(request);
		return future;
	}
	
	public ChannelFuture asynDoGet(ClientBootstrap client, String host, int port, String uri){
		SocketAddress remoteAddress = new InetSocketAddress(host, port);
		return asynDoGet(client, remoteAddress, uri);
	}
	
	public ChannelFuture asynDoPost(ClientBootstrap client, String host, int port, String uri, byte[] content){
		SocketAddress remoteAddress = new InetSocketAddress(host, port);
		return asynDoPost(client, remoteAddress, uri, content);
	}
	
	public ChannelFuture asynDoPost(ClientBootstrap client, String hostPort, String uri, byte[] content){
		String[] hp = hostPort.split(":");
		return asynDoPost(client, hp[0], new Integer(hp[1]), uri, content);
	}
	
	public ChannelFuture asynDoPost(ClientBootstrap client, SocketAddress address, String uri, byte[] content){
		ChannelFuture future = client.connect(address);
		Channel channel = future.awaitUninterruptibly().getChannel();
		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
				HttpMethod.POST, uri);
		request.setContent(ChannelBuffers.copiedBuffer(content));
		channel.write(request);
		return future;
	}
	
}
