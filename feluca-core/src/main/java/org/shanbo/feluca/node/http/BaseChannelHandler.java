package org.shanbo.feluca.node.http;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;





import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;
import org.shanbo.feluca.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BaseChannelHandler extends SimpleChannelHandler {
	
	static Logger log = LoggerFactory.getLogger(BaseChannelHandler.class);
	protected final static String CONTENT_TYPE = Strings.CONTENT_TYPE;


	protected Handlers handlers ;

	public BaseChannelHandler( Handlers handlers ) {
		this.handlers = handlers;
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
			HttpResponseUtil.setResponse(resp, "path :" + path, 
					"\"path not found! current_path : " + this.handlers.handlers.keySet() + "\"",
					HttpResponseStatus.BAD_REQUEST		);
		}else{
			handler.handle(nhr, resp);
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

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {

//		if (log.isTraceEnabled())
//			log.trace("Connection exceptionCaught:{}", e.getCause().toString());
		e.getChannel().close();
	}
	
	public void addHandlers(Handlers handlers){
		this.handlers = handlers;
	}
	
	/**
	 * override this method for your log content
	 * @param channel
	 * @param nhr
	 * @param resp
	 * @throws UnsupportedEncodingException
	 */
	protected void writeAccessLog( final Channel channel, HttpRequest req, 
			DefaultHttpResponse resp) throws UnsupportedEncodingException{
		String ip = channel.getRemoteAddress().toString();
		if( ip.startsWith("/") ) ip = ip.substring(1);
		
		String url = req.getUri();
		String responeContent = resp.getContent().toString( CharsetUtil.UTF_8 );
		
		log.info( "{}\t{}", ip + " " + url, responeContent );
		
	}
	
	public String toString(){
		return " handlers=" + handlers.toString();
	}
}
