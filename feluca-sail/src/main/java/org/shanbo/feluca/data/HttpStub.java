package org.shanbo.feluca.data;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpRequest;


public class HttpStub {
	
	public SimpleChannelHandler modelServerChannel(){
				
		return new BytesChannelHandler();
	}
	
	static class BytesChannelHandler extends SimpleChannelHandler{
		//TODO
		public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e){
			HttpRequest req = (HttpRequest)e.getMessage();
			String uri = req.getUri();
			if (uri.equals("update")){
				
			}else if (uri.equals("fetch")){
				
			}
		}
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e){
			
		}
	}
	
}
