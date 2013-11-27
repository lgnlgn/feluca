package org.shanbo.feluca.node.http;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;









import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import org.shanbo.feluca.common.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *  @Description HTTP SERVER
 *	@author shanbo.liang
 */
public abstract class BaseNioServer extends Server {
	final ChannelGroup allChannels = new DefaultChannelGroup(
			"nio-server");
	protected Logger log;

	protected ServerBootstrap bootstrap;
	protected ChannelFactory channelFactory = null;

	public BaseNioServer() {

	}

	/**
	 * init
	 * 
	 * Implement this {@link org.shanbo.feluca.common.Server} method
	 */
	public void init() {
		log = LoggerFactory.getLogger(this.serverName());
	}

	abstract protected ChannelUpstreamHandler finalChannelUpstreamHandler();

	protected ChannelPipelineFactory getChannelPipelineFactory() {
		return new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new  ObjectEncoder(),
						new ObjectDecoder(), finalChannelUpstreamHandler());
			}
		};
	}

	protected final SocketAddress getSocketAddress() {

		return new InetSocketAddress(this.getServerAddress().split(":")[0],
				Integer.parseInt(this.getServerAddress().split(":")[1]));
	}


	protected ChannelFactory createChannelFactory() {
		return new NioServerSocketChannelFactory(//es, es
		 Executors.newCachedThreadPool(),
		 Executors.newCachedThreadPool()
		);

	}

	/**
	 * start
	 * 
	 * Implement this {@link org.shanbo.feluca.common.Server} method
	 */
	public void preStart() throws Exception{
		this.channelFactory = this.createChannelFactory();

		bootstrap = new ServerBootstrap(channelFactory);
		bootstrap.setPipelineFactory(getChannelPipelineFactory());

		Channel serverChannel = bootstrap.bind(this.getSocketAddress());
		allChannels.add(serverChannel);
		log.info("[{}] started at : {} ", this.serverName() , this.getSocketAddress());

	}

	/**
	 * stop
	 * 
	 * Implement this {@link org.shanbo.feluca.common.Server} method
	 */
	public void postStop() throws Exception {
		ChannelGroupFuture closeFuture = allChannels.close();
		closeFuture.awaitUninterruptibly();
		
		if (channelFactory != null)
			channelFactory.releaseExternalResources();
	}

}
