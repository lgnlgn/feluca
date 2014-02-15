package org.shanbo.feluca.data;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.Handler;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.node.leader.LeaderNettyChannel;


public class HttpStub {
	
	public BaseChannelHandler dataServerChannel(){
		Handlers channelHandlers = new Handlers();
		BaseChannelHandler channel = new LeaderNettyChannel(channelHandlers);
		channelHandlers.addHandler(serverHandler());
		return channel;
	}
	
	
	public Handler serverHandler(){
		return new Handler() {
			
			public void handle(NettyHttpRequest req, DefaultHttpResponse resp) {
				// TODO Auto-generated method stub
			}
			
			public String getPath() {
				return null;
			}
		};
	}
	
	
}
