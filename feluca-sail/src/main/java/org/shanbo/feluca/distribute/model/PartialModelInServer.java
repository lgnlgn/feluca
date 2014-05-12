package org.shanbo.feluca.distribute.model;


import org.jboss.netty.buffer.ChannelBuffers;
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
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.shanbo.feluca.data.convert.DataStatistic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartialModelInServer {
	static Logger log = LoggerFactory.getLogger(PartialModelInServer.class);
	
	volatile float values[];//key value storage; since key=array_index, we need to use partitioner to convert id to index
	Partitioner partitioner;
	SimpleChannelHandler serverChannel;
	int thisBlock;
	
	public PartialModelInServer(GlobalConfig conf, int thisBlockId){
		serverChannel = new BytesChannelHandler();
		partitioner = conf.getPartitioner();
		init(conf.getModelServers().size(), 
				thisBlockId, 
				conf.getDataStatistic().getIntValue(DataStatistic.MAX_FEATURE_ID));
	}
	
	
	private void init(int allBlocks, int thisBlockId, int maxIds){
		int maxIdOfThisBlock = maxIds / allBlocks + 1;
		this.thisBlock = thisBlockId;
		values = new float[maxIdOfThisBlock + 1];
	}
	
	private void mergeModel(byte[] modelArray){
		for(int offset = 0 ; offset < modelArray.length ; offset += 8){
			int fid = BytesPark.yieldIdFrom4bytes(offset, modelArray);
			float deltaValue = BytesPark.yieldValueFrom8Bytes(offset, modelArray);
			int index = partitioner.featureIdToIndex(fid);
			values[index] += deltaValue;
		}
	}
	
	
	private byte[] fetchModel(byte[] idsArray){
		byte[] result = new byte[idsArray.length * 2];
		int rOffest = 0;
		for(int offset = 0 ; offset < idsArray.length; offset+=4, rOffest+= 8){
			int fid = BytesPark.yieldIdFrom4bytes(offset, idsArray);
			int index = partitioner.featureIdToIndex(fid);
			BytesPark.fillIntFloatToBytes(rOffest, fid, values[index], result);
		}
		return result;
	}
	
	
	class BytesChannelHandler extends SimpleChannelHandler{
		//TODO
		public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e){
			HttpRequest req = (HttpRequest)e.getMessage();
			HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
			String uri = req.getUri();
			if (uri.equals("/update")){
				byte[] idValues = req.getContent().array();
				mergeModel(idValues);
				resp.setStatus(HttpResponseStatus.CREATED);
			}else if (uri.equals("/fetch")){
				byte[] ids = req.getContent().array();
				byte[] result = fetchModel(ids);
				resp.setContent(ChannelBuffers.copiedBuffer(result));
			}
			boolean close = !HttpHeaders.isKeepAlive(req);

			resp.setHeader(HttpHeaders.Names.CONNECTION,
					close ? HttpHeaders.Values.CLOSE
							: HttpHeaders.Values.KEEP_ALIVE);
			
			ChannelFuture cf = e.getChannel().write(resp);

			if (close) 
				cf.addListener(ChannelFutureListener.CLOSE);
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e){
			e.getChannel().close();
		}
	}
	
	public SimpleChannelHandler getChannelForNetty(){
		return serverChannel;
	}
	public void saveModel(){
		//TODO
	}
}
