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
