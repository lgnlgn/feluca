package org.shanbo.feluca.node.leader;

import org.apache.zookeeper.KeeperException;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.BaseNioServer;
import org.shanbo.feluca.node.http.Handler;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.leader.handler.ClusterStatusRequest;
import org.shanbo.feluca.node.leader.handler.JobStatusHandler;
import org.shanbo.feluca.node.leader.handler.JobSubmitRequest;
import org.shanbo.feluca.util.ZKClient;
import org.slf4j.LoggerFactory;

public class LeaderServer extends BaseNioServer{

	Handlers handlers = new Handlers();
	final BaseChannelHandler channel = new LeaderNettyChannel(handlers);

	public String serverName() {
		return "feluca.leader";
	}

	@Override
	protected ChannelUpstreamHandler finalChannelUpstreamHandler() {
		return null;
	}

	public LeaderServer(){
		super();
		log = LoggerFactory.getLogger(LeaderServer.class);
	}

	public int defaultPort()
	{
		return 12020;
	}

	public void addHandler(Handler... hander){
		this.handlers.addHandler(hander);
	}


	protected ChannelPipelineFactory getChannelPipelineFactory(){
		return new ChannelPipelineFactory(){

			public ChannelPipeline getPipeline()
					throws Exception{

				// Create a default pipeline implementation.
				ChannelPipeline pipeline = Channels.pipeline();


				pipeline.addLast("decoder", new HttpRequestDecoder());
				pipeline.addLast("chunk" , new org.jboss.netty.handler.codec.http.HttpChunkAggregator(8888888));
				pipeline.addLast("encoder", new HttpResponseEncoder());
				pipeline.addLast("channel", channel);

				return pipeline;
			}
		};
	}


	@Override
	public String zkRegisterPath() {
		return Constants.ZK_LEADER_PATH ;
	}

	@Override
	public void preStart() throws Exception {
		ZKClient.get().createIfNotExist(Constants.ZK_CHROOT);
		ZKClient.get().createIfNotExist(Constants.ZK_LEADER_PATH);
		LeaderModule module = new LeaderModule();
		this.addHandler(new JobSubmitRequest(module));
		this.addHandler(new JobStatusHandler(module));
		this.addHandler(new ClusterStatusRequest(module));
		module.register(zkRegisterPath(), getServerAddress());
		super.preStart();
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
	}
}