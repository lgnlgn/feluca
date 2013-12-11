package org.shanbo.feluca.node.leader;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.datasys.DataServer;
import org.shanbo.feluca.datasys.ftp.DataFtpServer;
import org.shanbo.feluca.node.http.BaseChannelHandler;
import org.shanbo.feluca.node.http.BaseNioServer;
import org.shanbo.feluca.node.http.Handler;
import org.shanbo.feluca.node.http.Handlers;
import org.shanbo.feluca.node.request.ClusterStatusRequest;
import org.shanbo.feluca.node.request.JobKillRequest;
import org.shanbo.feluca.node.request.JobStatusHandler;
import org.shanbo.feluca.node.request.JobSubmitRequest;
import org.shanbo.feluca.util.ZKClient;
import org.slf4j.LoggerFactory;

public class LeaderServer extends BaseNioServer{
	LeaderModule module;
	DataServer dataServer;
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
//				pipeline.addLast("chunk" , new org.jboss.netty.handler.codec.http.HttpChunkAggregator(8888888));
				pipeline.addLast("encoder", new HttpResponseEncoder());
				pipeline.addLast("channel", channel);

				return pipeline;
			}
		};
	}


	@Override
	public String zkRegisterPath() {
		return Constants.Base.ZK_LEADER_PATH ;
	}

	@Override
	public void preStart() throws Exception {
		ZKClient.get().createIfNotExist(Constants.Base.ZK_CHROOT);
		ZKClient.get().createIfNotExist(zkRegisterPath());
		module = new LeaderModule();
		this.addHandler(new JobSubmitRequest(module));
		this.addHandler(new JobStatusHandler(module));
		this.addHandler(new ClusterStatusRequest(module));
		this.addHandler(new JobKillRequest(module));
		module.init(zkRegisterPath(), getServerAddress());
		super.preStart();
		
		ZKClient.get().createIfNotExist(Constants.Base.FDFS_ZK_ROOT);
		dataServer = new DataFtpServer();
		dataServer.start();
		
	}

	@Override
	public void postStop() throws Exception {
		module.shutdown();
		super.postStop();
		dataServer.stop();
	}
	
	/**
	 * test
	 * @param args
	 */
	public static void main(String[] args) {
		LeaderServer server = new LeaderServer();
		server.start();
	}
	
}
