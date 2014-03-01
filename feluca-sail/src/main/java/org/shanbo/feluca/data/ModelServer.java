package org.shanbo.feluca.data;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.BaseNioServer;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.leader.LeaderNettyChannel;

/**
 * server will be initialized in ModelView
 * @author lgn
 *
 */
public class ModelServer extends BaseNioServer{
	
	String zkPath = Constants.Algorithm.ZK_ALGO_CHROOT + "/test";
	ModelInServer model ;
	SimpleChannelHandler modelChannel;
	
	@Override
	public String serverName() {
		return "VectorDataModelServer";
	}

	@Override
	public int defaultPort() {
		return 12800;
	}

	@Override
	public String zkRegisterPath() {
		return zkPath;
	}

	@Override
	public void preStart() throws Exception {
		modelChannel = model.getChannelForNetty();
		
	}

	@Override
	public void postStop() throws Exception {
		super.postStop(); //shutdown channels
		
	}

	@Override
	protected ChannelUpstreamHandler finalChannelUpstreamHandler() {
		return null;
	}
	/**
	 * handle http 
	 */
	protected ChannelPipelineFactory getChannelPipelineFactory(){
		return new ChannelPipelineFactory(){

			public ChannelPipeline getPipeline()
					throws Exception{

				// Create a default pipeline implementation.
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("decoder", new HttpRequestDecoder());
				pipeline.addLast("encoder", new HttpResponseEncoder());
				pipeline.addLast("channel", modelChannel);

				return pipeline;
			}
		};
	}
}
