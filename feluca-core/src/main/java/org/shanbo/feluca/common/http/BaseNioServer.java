package org.shanbo.feluca.common.http;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
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
import org.shanbo.feluca.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public abstract class BaseNioServer implements Server {
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
	 * Implement this {@link org.jarachne.network.http.woyo.search.query.common.Server} method
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

	public String getServerAddress() {

		String myip = "0.0.0.0";
		try {
			myip = NetworkUtils.getFirstNonLoopbackAddress(
					NetworkUtils.StackType.IPv4).getHostAddress();
		} catch (SocketException e) {
		}
		Config c = Config.get();
		if (c.get(this.serverName() + ".bind-address") == null || c.get(this.serverName() + ".bind-port") == null){
			System.out.println("YOURSERVER.bind-address and port not set!!!! use default value");
		}
		String ip = c.get(this.serverName() + ".bind-address", myip);
		int port = c.getInt(this.serverName() + ".bind-port", defaultPort());
		return ip + ":" + port;
	}
	protected int defaultPort()
	{
		return 21001;
	}

	protected ChannelFactory createChannelFactory() {
//		ExecutorService es = Executors.newCachedThreadPool();

		return new NioServerSocketChannelFactory(//es, es
		 Executors.newCachedThreadPool(),
		 Executors.newCachedThreadPool()
		// new MemoryAwareThreadPoolExecutor(4, 0, 100000000)

		);

	}

	/**
	 * start
	 * 
	 * Implement this {@link org.jarachne.network.http.woyo.search.query.common.Server} method
	 */
	public void start() {
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
	 * Implement this {@link org.jarachne.network.http.woyo.search.query.common.Server} method
	 */
	public void stop() {
		ChannelGroupFuture closeFuture = allChannels.close();
		closeFuture.awaitUninterruptibly();
		
		if (channelFactory != null)
			channelFactory.releaseExternalResources();
	}

}
