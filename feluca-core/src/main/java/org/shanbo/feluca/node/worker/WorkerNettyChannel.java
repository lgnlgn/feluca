package org.shanbo.feluca.node.worker;

import java.util.Set;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.Handler;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.http.HttpResponseUtil;
import org.shanbo.feluca.node.http.NettyHttpRequest;
import org.shanbo.feluca.util.Strings;

import com.alibaba.fastjson.JSONArray;

public class WorkerNettyChannel extends BaseChannelHandler{


	public WorkerNettyChannel(Handlers handlers) {
		super(handlers);
		
	}

	public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e)
			throws Exception {
		// common http request
		HttpRequest req = (HttpRequest)e.getMessage();
		NettyHttpRequest nhr = new NettyHttpRequest(req);
		String path = nhr.path();

		Handler handler = handlers.getHandler(path);
		DefaultHttpResponse resp = new DefaultHttpResponse(req.getProtocolVersion(), HttpResponseStatus.OK);
		if (handler == null){
			Set<String> handlerPaths = this.handlers.getHandlerPaths();
			JSONArray ja = new JSONArray();
			ja.addAll(handlerPaths);
			HttpResponseUtil.setResponse(resp, "request path :" + path, 
					Strings.keyValuesToJson("code",404, "available_path", ja),
					HttpResponseStatus.BAD_REQUEST	);
		}else {
			if (req.getMethod().equals(HttpMethod.POST)){
				String action = nhr.contentAsString();
				if (Strings.isNetworkMsg(action))
					handler.handle(nhr, resp);
				else{
					HttpResponseUtil.setResponse(resp, "request path :" + path, 
							Strings.keyValuesToJson("code",404, "ciphertext error", action),
							HttpResponseStatus.BAD_REQUEST	);
				}

			}else{
				HttpResponseUtil.setResponse(resp, "request path :" + path, 
						Strings.keyValuesToJson("code",404, "allow http method", "POST"),
						HttpResponseStatus.BAD_REQUEST	);
			}
		}
		resp.setHeader(HttpHeaders.Names.CONTENT_TYPE, Strings.CONTENT_TYPE);
		resp.setHeader("Content-Length", resp.getContent().readableBytes());

		boolean close = !HttpHeaders.isKeepAlive(req);

		resp.setHeader(HttpHeaders.Names.CONNECTION,
				close ? HttpHeaders.Values.CLOSE
						: HttpHeaders.Values.KEEP_ALIVE);

		ChannelFuture cf = e.getChannel().write(resp);

		if (close) 
			cf.addListener(ChannelFutureListener.CLOSE);
		writeAccessLog(e.getChannel(), req, resp);

	}
}
