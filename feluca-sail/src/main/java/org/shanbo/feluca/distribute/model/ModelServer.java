package org.shanbo.feluca.distribute.model;

import java.net.UnknownHostException;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.shanbo.feluca.common.ClusterUtil;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.common.Constants.Network;
import org.shanbo.feluca.common.Server;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.BaseNioServer;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.leader.LeaderNettyChannel;
import org.shanbo.feluca.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * server will be initialized in ModelView
 * @author lgn
 *
 */
public class ModelServer extends BaseNioServer{
	
	final static int PORT = 12800;
	Logger log = LoggerFactory.getLogger(ModelServer.class);
	String zkPath = Constants.Algorithm.ZK_ALGO_CHROOT + "/test";
	PartialModelInServer model ;
	GlobalConfig conf;
	String host ;
	int modelSegmentID;
	
	public ModelServer(GlobalConfig conf, int modelSegmentId){
		this.conf = conf;
		this.modelSegmentID = modelSegmentId;
	}
	
	@Override
	public String serverName() {
		return "VectorDataModelServer";
	}

	@Override
	public int defaultPort() {
		return PORT;
	}

	@Override
	public String zkRegisterPath() {
		return zkPath;
	}

	@Override
	public void preStart() throws Exception {
		ClusterUtil.getWorkerList();
		model = new PartialModelInServer(conf, modelSegmentID);
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
				pipeline.addLast("channel", model.getChannelForNetty());

				return pipeline;
			}
		};
	}
}
