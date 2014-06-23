package org.shanbo.feluca.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.shanbo.feluca.util.concurrent.JMXConfigurableThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;

import org.apache.zookeeper.data.Stat;





/**
 * @version 1.0
 */
public class ZKClient
{
	//    static private ESLogger log = Loggers.getLogger(ZKClient.class);
	private static final Logger log = LoggerFactory.getLogger(ZKClient.class);
	private volatile ZooKeeper zooKeeper;
	static private ZKClient CLIENT;
	private ExecutorService watchExecutorService = JMXConfigurableThreadPoolExecutor.newCachedThreadPool("zk-task");// Executors.newCachedThreadPool();
	private ConcurrentMap<Object, WatchBag> watches = new ConcurrentHashMap<Object, WatchBag>();
	private Map<String,String> ephemeralNodes = new ConcurrentHashMap<String,String>();
	private List<RetryRun> tries=Collections.synchronizedList( new LinkedList<RetryRun>());
	private List<RetryRun> watchTries=Collections.synchronizedList( new LinkedList<RetryRun>());

	
	private String[] connectAddress = null;
	
	final Watcher defaultWatch = new Watcher()
	{
		public void process(WatchedEvent e)
		{
			log.info(e.getPath());
			//System.out.println(e.getPath() + " " + e.toString());

			if( e.getState()==  Watcher.Event.KeeperState.Expired)
			{

				handleExpired();
			}
			else if( e.getState()==  Watcher.Event.KeeperState.Disconnected)
			{
				log.warn("ZKDisconnected! {}", e.toString());
			}
			else if( e.getState()==  Watcher.Event.KeeperState.SyncConnected)
			{
				log.warn("ZKSyncConnected! {}", e.toString());
				ZKClient.this.handleSynConnected();
			}


		}
	};

	private static interface RetryRun
	{
		public void run()
				throws KeeperException, InterruptedException;
	}

	private static class WatchBag
	{
		WatchBag(Object watch,String path,String payload)
		{
			this.path=path;
			this.payload=payload;
			this.watch=watch;
		}
		private String path;
		private String payload;
		volatile Object watch;

		private long lastid=-999;


	}

	public static abstract class ChildrenWatcher
	{

		final private Set<String> children = new HashSet<String>();
		/**
		 *can  do long task in this callback
		 * @param node
		 */
		public abstract void nodeAdded(String node);
		/**
		 *can  do long task in this callback
		 * @param node
		 */
		public abstract void nodeRemoved(String node);
		public boolean triggerInNewThread()
		{
			return true;
		}

		final synchronized public List<String> getChildren()
		{
			return new ArrayList<String>(this.children);
		}

		synchronized final private void childrenCome(List<String> c)
		{
			for (  String s: c)
			{

				if (this.children.contains(s))
				{
					continue;
				}
				else
				{
					this.children.add(s);
					final String fs=s;
					if(this.triggerInNewThread())
					{
						ZKClient.get(). watchExecutorService.submit(new Runnable()
						{

							public void run()
							{
								try
								{
									nodeAdded(fs);
								}
								catch (Throwable e)
								{
									log.error("Error while submit node added:{}", e, fs );
								}
							}
						});
					}
					else
					{
						try
						{
							nodeAdded(fs);
						}
						catch (Throwable e)
						{
							log.error("Error while node added:{}", e, fs);
						}
					}

				}
			}
			List<String> toRemove = new ArrayList<String>();
			for (String s: this.children)
			{
				if (c.contains(s))
				{
					continue;
				}
				else
				{
					toRemove.add(s);
				}
			}
			for (String s: toRemove)
			{
				this.children.remove(s);
				final String fs=s; 
				if(this.triggerInNewThread())
				{
					ZKClient.get(). watchExecutorService.submit(new Runnable()
					{

						public void run()
						{
							try
							{
								nodeRemoved(fs);

							}
							catch (Exception e)
							{
								log.error("Error while node removed:{}", e,fs);

							}
						}
					});
				}
				else
				{
					try
					{
						nodeRemoved(fs);

					}
					catch (Exception e)
					{
						log.error("Error while node removed:{}", e,fs);

					}
				}


			}


		}


	}


	public static interface LongValueWatcher
	{
		void valueChaned(long l);

	}
	public static interface StringValueWatcher
	{
		void valueChaned(String l);
	}


	public static interface MasterWatcher
	{
		/**
		 *Master变更为该server，通过比较与本server是否相同判断自己是否master,
		 * 如果调用时给出的servername为null，这里的参数就可以为null
		 * @param serverName
		 */
		void masterChangeTo(String serverName);

		void exceptionCaught(Throwable t);
	}


	private static class InterMasterWatcher
	{
		final WatchBag watch;
		private String old;
		private String path;

		public InterMasterWatcher(WatchBag watch)
		{
			this.watch = watch;

		}

		private void masterChangeTo(final String serverName)
		{

			MasterWatcher mw = (MasterWatcher)watch.watch;
			if (mw != null)
			{
				mw.masterChangeTo(serverName);
			}



		}

		private void setMaster(String name)
		{

			if (old != null && old.equals(name))
			{
				log.debug("It's same,Not set Master:{},path:{}", name, path);
			}
			else
			{
				old = name;
				log.debug("Setting Master:{},path:{}", name, path);
				this.masterChangeTo(name);


			}

		}

	}

	private ZKClient()
	{
		try
		{
			this.init();
		}
		catch (IOException ex)
		{
			log.error("Error connect ZK", ex);
		}

	}

	/**
	 *在hbase内部mapreduce时使用
	 * @param conf
	 */
	//    public ZKClient(HBaseConfiguration conf)
	//        throws IOException
	//    {
	//        Properties properties = HQuorumPeer.makeZKProps(conf);
	//        String clientPort = null;
	//        List<String> servers = new ArrayList<String>();
	//
	//        // The clientPort option may come after the server.X hosts, so we need to
	//        // grab everything and then create the final host:port comma separated list.
	//        boolean anyValid = false;
	//        for (Map.Entry<Object, Object> property: properties.entrySet())
	//        {
	//            String key = property.getKey().toString().trim();
	//            String value = property.getValue().toString().trim();
	//            if (key.equals("clientPort"))
	//            {
	//                clientPort = value;
	//            }
	//            else if (key.startsWith("server."))
	//            {
	//                String host = value.substring(0, value.indexOf(':'));
	//                servers.add(host);
	//                try
	//                {
	//                    InetAddress.getByName(host);
	//                    anyValid = true;
	//                }
	//                catch (UnknownHostException e)
	//                {
	//                    log.warn(StringUtils.stringifyException(e));
	//                }
	//            }
	//        }
	//
	//        if (!anyValid)
	//        {
	//            log.error("no valid quorum servers found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
	//            return;
	//        }
	//
	//        if (clientPort == null)
	//        {
	//            log.error("no clientPort found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
	//            return;
	//        }
	//
	//        if (servers.isEmpty())
	//        {
	//            log.error("No server.X lines found in conf/zoo.cfg. HBase must have a " + "ZooKeeper cluster configured for its operation.");
	//
	//            return;
	//        }
	//
	//        StringBuilder hostPortBuilder = new StringBuilder();
	//        for (int i = 0; i < servers.size(); ++i)
	//        {
	//            String host = servers.get(i);
	//            if (i > 0)
	//            {
	//                hostPortBuilder.append(',');
	//            }
	//            hostPortBuilder.append(host);
	//            hostPortBuilder.append(':');
	//            hostPortBuilder.append(clientPort);
	//        }
	//
	//
	//        zooKeeper = new ZooKeeper(hostPortBuilder.toString(), conf.getInt("zookeeper.session.timeout", 60000), defaultWatch);
	//
	//
	//    }
	/**
	 * <p>本类为单例，一旦调用就没法改变，如果你需要指定自己的zk配置路径，在使用本类之前先调用BaseConfig.setConfigFrom()</p>
	 * @return
	 */
	public synchronized static ZKClient get()
	{
		if (CLIENT == null)
		{
			CLIENT = new ZKClient();
		}
		//	throw new RuntimeException("here");
		return CLIENT;
	}

	private void init()
			throws IOException
			{
		//        zooKeeper = new ZooKeeper(SystemConfig.getProperty("zk.quorum"), Integer.parseInt(SystemConfig.getProperty("zk.session.timeout", "30000")), defaultWatch);
		String zkQuorum = Config.get().get("zk.quorum", "localhost:2181");

//		zooKeeper = new ZooKeeper(StringUtils.join(zkHostList, ","), new Integer(BaseConfig.getValue("zk.session.timeout", "30000")), defaultWatch);
		zooKeeper = new ZooKeeperRetry(zkQuorum, new Integer(Config.get().get("zk.session.timeout", "30000")), defaultWatch);
	}



	private void handleSynConnected()
	{
		for(RetryRun r:this.tries)
		{
			try
			{
				r.run();
			}
			catch ( Exception e)
			{
				log.error("Error while handleSynConnected", e);
			}
		}
		this.tries.clear();
		for(RetryRun r:this.watchTries)
		{
			try
			{
				r.run();
			}
			catch ( Exception e)
			{
				log.error("Error while handleSynConnected", e);
			}
		}
		this.watchTries.clear();
	}

	/**
	 * 当到ZK的连接超时触发
	 *  TODO:
	 */
	synchronized protected void handleExpired()
	{

		if(this.zooKeeper.getState()==ZooKeeper.States.CONNECTED)
		{
			log.trace("Already handling ZK Expired");
			return;
		}
		try
		{
			this.zooKeeper.close();

		}
		catch ( Exception e)
		{
			log.error("Error while close old zk", e);
		}
		log.info("Handling ZK Expired");


		try
		{
			this.init();
			this.watchTries.clear();



		}
		catch (Exception e)
		{
			log.error("Error while init ZK ,in handle expored", e);
		}

		for(Map.Entry<String,String> e:this.ephemeralNodes.entrySet())
		{
			try
			{

				this.registerEphemeralNode(e.getKey(), e.getValue());
			}
			catch(KeeperException.ConnectionLossException ex)
			{
				final String ek=e.getKey();
				final String ev=e.getValue();
				this.tries.add(new RetryRun()
				{
					public void run()
							throws KeeperException, InterruptedException
							{
						registerEphemeralNode(ek, ev);
							}
				});
			}
			catch ( Exception f)
			{
				log.warn("Error while reregisterEphemeralNode,handleExpired", f);
			}
		}
		try
		{

			for(WatchBag bag:this.watches.values())
			{
				if(bag.watch==null)
				{
					continue;
				}
				this.addWatch(bag);
			}
		}
		catch (Exception e)
		{
			log.error("Error while init ZK ,in handle expored", e);
		}

	}

	public String dump(String path,int tabCount) throws InterruptedException, KeeperException
	{
		StringBuilder sb=new StringBuilder();
		List<String> cl= this.getChildren(path);

		for(String c:cl)
		{
			String cp=path +(path.endsWith("/")?"": "/")+c;
			for(int i=0;i<tabCount;i++)
			{
				sb.append("\t");
			}
			//modified by junsen_ye 20100928 节点数据为null时，此处抛异常
			byte [] data = this.getData(cp);
			String info = "";
			if(null != data && data.length > 0){
				info = new String(data);
			}
			//end modified
			sb.append(c).append("{").append(info).append("}\n");
			sb.append( this.dump(cp, tabCount+1));

		}
		return sb.toString();
	}

	public void unRegisterEphemeralNode(final String path,   String hostname)
			throws KeeperException, InterruptedException
			{
		this.ephemeralNodes.remove(path);
		final	String fullpath=path+"/"+hostname;
		try{
			this.forceDelete(fullpath);
		}
		catch(KeeperException.ConnectionLossException e)
		{
			log.warn("ConnectionLossException:{},{}", path,hostname);
			this.tries.add(new RetryRun(){
				public void run()
						throws KeeperException, InterruptedException
						{
					if(!ephemeralNodes.containsKey(path))
					{
						forceDelete(fullpath);
					}
						}
			});

		}
			}

	public void registerEphemeralNode(String path, String nodename)
			throws KeeperException, InterruptedException
			{
		// this.createIfNotExist(path);
		if(nodename==null)
		{
			throw new java.lang.NullPointerException("hostname is null,path:"+path);
		}

		String sb = new StringBuilder(path).append("/").append(nodename).toString();
		try
		{
			this.zooKeeper.create(sb, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
		catch (KeeperException.NodeExistsException ke)
		{
			log.warn("Node {} exisit,delete and create again", sb);


			this.forceDelete(sb);

			this.zooKeeper.create(sb, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		}

		ephemeralNodes.put(path, nodename);


			}



	//    private AtomicReference checkExist(Object cw,String path,  boolean first)
	//    {
	//        if (cw == null)
	//        {
	//            return null;
	//        }
	//        AtomicReference ar = null;
	//        if (first)
	//        {
	//            ar = new AtomicReference(cw);
	//            this.watches.put(cw, ar);
	//        }
	//        else
	//        {
	//            ar = this.watches.get(cw);
	//
	//        }
	//
	//        return ar;
	//    }

	/**
	 *whill stackoverflow? need check
	 * @param path
	 * @param cw
	 */
	public void watchChildren(final String path, ChildrenWatcher cw)
	{
		WatchBag bag=new WatchBag(cw,path,null);
		this.watches.put(cw, bag);
		this.__watchChildren(bag);
	}


	private void __watchChildren(final WatchBag bag)
	{
		ChildrenWatcher cw=(ChildrenWatcher)bag.watch;
		if(cw==null)
		{
			return ;
		}
		final Watcher w=new Watcher()
		{


			public void process(WatchedEvent we)
			{

				if (we.getState() == Event.KeeperState.Expired)
				{

					handleExpired();
				}
				else if (we.getType() == Event.EventType.NodeChildrenChanged)
				{

					__watchChildren(bag);

				}
				else if (we.getType() == Event.EventType.NodeCreated)
				{
					__watchChildren(bag);
				}
				else if (we.getType() == Event.EventType.NodeDeleted)
				{
					__watchChildren(bag);
				}

				else if (we.getType() == Event.EventType.NodeDataChanged)
				{
					log.warn("watch childern NodeDataChanged:{}", bag.path);

				}

			}
		};


		try
		{	Stat stat=new Stat();
		java.util.List<java.lang.String> result = zooKeeper.getChildren(bag.path,w ,stat);
		if(bag.lastid==stat.getCversion())
		{
			log.warn("children watch get a dupmxxid:{}", bag.lastid);
			return;
		}
		bag.lastid=stat.getCversion();
		cw.childrenCome(result);
		}
		catch(KeeperException.ConnectionLossException e)
		{
			log.debug("watch children  ConnectionLossException{}", bag.path);
			this.watchTries.add(new RetryRun(){
				public void run()
						throws KeeperException, InterruptedException
						{
					if(bag.watch!=null)
						zooKeeper.getChildren(bag.path, w, null);
						}
			});
		}
		catch (KeeperException e)
		{
			log.error("watch children KeeperException:{}", bag.path);
		}
		catch ( Exception e)
		{
			log.error("watch children Exception:{}", e,bag.path);
		}
	}


	public void watchStrValueNode(String path,StringValueWatcher watch)
	{
		WatchBag bag=new WatchBag(watch,path,null);
		this.watches.put(watch, bag);
		this.__watchStrValueNode(bag);
	}
	private String  __watchStrValueNode(final WatchBag bag)
	{
		StringValueWatcher lw=(StringValueWatcher)bag.watch;
		if(lw==null)
		{
			return null;
		}

		final  Watcher w = new Watcher()
		{
			public void process(org.apache.zookeeper.WatchedEvent we)
			{
				if (we.getState() == Event.KeeperState.Expired)
				{
					handleExpired();
				}
				else if (we.getType() == Event.EventType.NodeDeleted)
				{

				}
				else if (we.getType() == Event.EventType.NodeCreated)
				{
					if(bag.watch!=null)
						__watchStrValueNode(bag);
				}
				else if (we.getType() == Event.EventType.NodeDataChanged)
				{
					final StringValueWatcher lw = (StringValueWatcher) bag.watch;
					if (lw != null)
					{
						final String s = __watchStrValueNode(bag);
						if(s!=null)
						{
							new Thread(new Runnable()
							{
								public void run()
								{
									lw.valueChaned(s);
								}
							}, "StringWatcher-trigger-thread").start();
						}
					}
				}
			}
		};



		try
		{
			Stat stat=new Stat();
			String ss= new String(zooKeeper.getData(bag.path, w, stat));
			if(bag.lastid==stat.getMzxid())
			{
				log.warn("str watch get a dupmxxid:{}", bag.lastid);
				return null;
			}
			bag.lastid=stat.getMzxid();
			return ss;


		}
		catch (KeeperException.ConnectionLossException e)
		{
			log.debug("Str watch ConnectionLossException:{}", bag.path);
			this.watchTries.add(new RetryRun(){
				public void run()
						throws KeeperException, InterruptedException
						{	
					if(bag.watch!=null)
						zooKeeper.getData(bag.path, w, null);
						}
			});

		}
		catch (KeeperException e)
		{
			log.error(" watchStrValueNode KeeperException:{}", bag.path);
		}
		catch (Exception e)
		{
			log.error("watchStrValueNode Exception:{}",e, bag.path);
		}
		return null;
	}
	/**
	 *
	 * @param path
	 * @param watch
	 * @return
	 */
	public long watchLongValueNode(final String path, final LongValueWatcher watch)
	{
		WatchBag bag=new WatchBag(watch,path,null);
		this.watches.put(watch, bag);
		return this.__watchLongValueNode(bag);
	}

	private long __watchLongValueNode(final WatchBag bag)

	{	
		LongValueWatcher lw=(LongValueWatcher)bag.watch;
		if(lw==null)
		{
			return  Long.MAX_VALUE;
		}

		final  Watcher w = new Watcher()
		{
			public void process(org.apache.zookeeper.WatchedEvent we)
			{
				//  System.out.println(we);
				if (we.getState() == Event.KeeperState.Expired)
				{
					handleExpired();
				}
				else if (we.getType() == Event.EventType.NodeDeleted)
				{
					log.warn("NodeDeleted:{}", bag.path);
				}
				else if (we.getType() == Event.EventType.NodeCreated)
				{

					if ( bag.watch != null)
					{
						__watchLongValueNode(bag);
					}

				}
				else if (we.getType() == Event.EventType.NodeDataChanged)
				{ 
					final LongValueWatcher lw = (LongValueWatcher) bag.watch;
					if (lw != null)
					{
						final long lv = __watchLongValueNode(bag);
						if(lv!=Long.MAX_VALUE)
						{
							new Thread(new Runnable()
							{
								public void run()
								{
									lw.valueChaned(lv);
								}
							}, "LongWatcher-trigger-thread").start();
						}
					}
				}
			}
		};


		try
		{
			Stat stat=new Stat();
			String s = new String(this.zooKeeper.getData(bag.path, w, stat));
			if(bag.lastid==stat.getMzxid())
			{
				log.warn("long watch get a dupmxxid:{}", bag.lastid);
				return Long.MAX_VALUE;
			}
			bag.lastid=stat.getMzxid();
			long lv = Long.parseLong(s);

			return lv;

		}
		catch (KeeperException.ConnectionLossException e)
		{
			log.debug("ConnectionLossException{}", bag.path);
			this.watchTries.add(new RetryRun(){
				public void run()
						throws KeeperException, InterruptedException
						{
					if(bag.watch!=null)
						zooKeeper.getData(bag.path, w, null);
						}
			});

		}
		catch (KeeperException e)
		{
			log.error("KeeperException:{}", bag.path);
		}
		catch (InterruptedException e)
		{
			log.error("watchTimeNode InterruptedException:{}", bag.path);
		}
		catch (NumberFormatException e)
		{
			log.error("watchTimeNode NumberFormatException:{}", bag.path);
		}
		catch ( Exception e)
		{
			log.error("watchTimeNode Exception:{}",e, bag.path);
		}
		return Long.MAX_VALUE;


	}


	public void addWatch(WatchBag bag)
	{
		Object watch = bag.watch;
		if (watch instanceof ZKClient.MasterWatcher)
		{
			this.watchMaster(bag.path, bag.payload, (MasterWatcher) watch);
		}
		else if (watch instanceof ZKClient.LongValueWatcher)
		{
			this.watchLongValueNode(bag.path,  (LongValueWatcher) watch);
		}
		else if (watch instanceof ZKClient.ChildrenWatcher)
		{
			watchChildren(bag.path,  (ChildrenWatcher) watch);
		}
		else if (watch instanceof ZKClient.StringValueWatcher)
		{
			this.watchStrValueNode(bag.path,  (StringValueWatcher) watch);
		}

		else
		{
			throw new RuntimeException("Unknown Watch Type," + watch.getClass() + ":" + watch.toString() + " path:" + bag.path);
		}

	}

	public void destoryWatch(Object watch)
	{
		if(watch==null)
		{
			return;
		}
		WatchBag ar = this.watches.remove(watch);
		if (ar != null)
		{
			ar.watch=null;//(null);

		}
		if(watch instanceof ChildrenWatcher 
				|| watch instanceof LongValueWatcher
				|| watch  instanceof MasterWatcher
				|| watch  instanceof StringValueWatcher

				)
		{
		}
		else
		{
			throw new RuntimeException(watch.getClass()+ "Not a valid watch");
		}

	}
	public void unwatchMaster(final String path, final String serverAddress, final MasterWatcher watch)
	{
		this.destoryWatch(watch);
		if(serverAddress!=null)
		{
			this.deleteMaster(path, serverAddress);
		}
	}
	private void deleteMaster(final String path, final String serverAddress )
	{
		if (serverAddress != null)
		{
			try
			{
				byte[] master = this.zooKeeper.getData(path, false, null);
				String m = new String(master);
				if (m.equals(serverAddress))
				{
					zooKeeper.delete(path, -1);
				}

			}
			catch(KeeperException.NoNodeException e)
			{
				//it's ok
			}
			catch(KeeperException.ConnectionLossException e)
			{
				//			    log.warn("ConnectionLossException while deleteMaster: path{},address:{}",e, path,serverAddress);
				log.warn("ConnectionLossException while deleteMaster: path{},address:{}", path,serverAddress);
				this.tries.add(new RetryRun()
				{
					public void run()
					{
						log.debug("RetryRundeleteMaster: path{},address:{}", path,serverAddress);

						deleteMaster(path,serverAddress);
					}
				});
			}
			catch (Exception e)
			{
				log.error("Error while deleteMaster (line 1014): path{},address:{}", path,serverAddress);
				//				log.error("Error while deleteMaster: path{},address:{}",e, path,serverAddress);
			}
		}
	}
	public void watchMaster(  String path,   String serverAddress,   MasterWatcher watch)
	{


		this.deleteMaster(path,serverAddress);

		WatchBag bag=new WatchBag(watch,path,serverAddress);
		this.watches.put(watch, bag);

		__watchMaster(bag);


	}

	/**
	 *
	 * @param path 要Watch的节点的路径
	 * @param serverAddress：服务器名，如果null，只watch，不set
	 * @param watch
	 */
	final private void __watchMaster(final WatchBag bag)
	//throws KeeperException, InterruptedException
	{
		MasterWatcher watch=(MasterWatcher)bag.watch;
		if(watch==null)
		{
			return ;
		}



		final InterMasterWatcher iw = new InterMasterWatcher(bag);
		iw.path = bag.path;
		final  Watcher w = new Watcher()
		{
			public void process(org.apache.zookeeper.WatchedEvent we)
			{
				if (we.getState() == Event.KeeperState.Expired)
				{
					handleExpired();
				}
				else if (we.getType() == Event.EventType.NodeDeleted)
				{


					if (bag.payload == null)
					{
						iw.setMaster(null);
					}
					watchExecutorService.submit(new Runnable()
					{
						public void run()
						{
							try
							{
								Thread.currentThread();
								Thread.sleep((long) (300));
							}
							catch (InterruptedException e)
							{
							}
							__watchMaster(bag);
						}
					});
					//否则不能删除Cluster

				}
				else if (we.getType() == Event.EventType.NodeCreated)
				{
					__watchMaster(bag);
				}
				else if (we.getType() == Event.EventType.NodeDataChanged)
				{
					__watchMaster(bag);
					log.error("Node data should not change:{}", we);
				}
			}
		};

		try
		{

			try
			{
				Stat stat=new Stat();
				byte[] master = this.zooKeeper.getData(bag.path, false, stat);
				if(stat.getMzxid()==bag.lastid)
				{
					log.warn("master watch Get a dupmxxid:{}", bag.lastid);
				}
				bag.lastid=stat.getMzxid();

				iw.setMaster(new String(master));
			}
			catch (KeeperException.NoNodeException e)
			{
				String serverAddress=bag.payload;
				if (serverAddress!= null)
				{
					try
					{
						this.zooKeeper.create(bag.path,serverAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
						iw.setMaster(serverAddress);

					}
					catch (KeeperException.NodeExistsException ee)
					{
						//刚才没有，现在有了，zk会触发的，所以忽略
					}
					catch (KeeperException.NoNodeException ee)
					{
						//没有父节点，在删除schema
						watch.exceptionCaught(ee);
					}

				}
				else
				{
					iw.setMaster(null);
				}
			}

		}
		catch (Exception e)
		{
			log.error("Error while get Master", e);
			//  System.exit(-1);
			watch.exceptionCaught(e);

		}
		try
		{
			this.zooKeeper.exists(bag.path, w);
		}
		catch(KeeperException.ConnectionLossException e)
		{
			this.watchTries.add(new RetryRun()
			{
				public void run()
						throws KeeperException, InterruptedException
						{
					zooKeeper.exists(bag.path, w);
						}
			});
		}
		catch (Exception ex)
		{
			log.warn("ZK Error while watch Master", ex);
		}


	}

	public void forceDelete(String path)
			throws KeeperException, InterruptedException
			{
		try
		{
			List<String> ss = this.getChildren(path);

			for (String s: ss)
			{
				this.forceDelete(path + "/" + s);
			}


			this.zooKeeper.delete(path, -1);
		}

		catch (KeeperException.NoNodeException e)
		{
			//do nothinh
		}

			}

	public List<String> getChildren(String path)
			throws InterruptedException, KeeperException
			{
		return zooKeeper.getChildren(path, false);

			}

	public byte[] getData(String path)
			throws KeeperException, InterruptedException
			{
		return zooKeeper.getData(path, false, null);
			}


	public void createIfNotExist(String p)
			throws KeeperException, InterruptedException
			{
		String[] paths = p.split("/");
		String path = "";

		for (int i = 0; i < paths.length; i++)
		{
			if (paths[i].equals(""))
			{
				continue;
			}
			path = path + "/" + paths[i];


			if (null == zooKeeper.exists(path, false))
			{
				zooKeeper.create(path, new byte[]
						{ }, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
			}

	public String create(String path, byte[] data)
			throws KeeperException, InterruptedException
			{
		return zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			}

	public String getStringData(String path)
			throws KeeperException, InterruptedException
			{

		try
		{
			return new String(zooKeeper.getData(path, false, null));
		}
		catch (KeeperException.NoNodeException ke)
		{

			return null;
		}
			}


	public Map<String, String> loadMap(String path)
			throws InterruptedException, KeeperException
			{
		List<String> ss = getChildren(path);

		Map<String, String> v = new TreeMap<String, String>();
		for (String s: ss)
		{
			v.put(s, getStringData(path + "/" + s));
		}
		return v;
}
	/**
	 * DO NOT cache ZooKeeper Object!!!
	 * @return
	 */
	public ZooKeeper getZooKeeper()
	{
		return zooKeeper;
	}




	public static void main(String[] ss)
			throws IOException, InterruptedException, KeeperException
			{
		ZKClient.get().forceDelete("/search0");
		ZKClient.get().forceDelete("/SearchMan/search0");
		//
		//        final ZKClient client = ZKClient.get();
		//		Thread t=new Thread(new Runnable()
		//							{
		//
		//				public void run()
		//				{
		//					try
		//					{
		//						Thread.currentThread().sleep(Integer.MAX_VALUE);
		//					}
		//					catch (InterruptedException e)
		//					{
		//					}
		//				}
		//			});
		//		
		//	t.start();
		//      // String s= client.dump("/", 1);
		//      // System.out.println(s);
		//        client.watchChildren("/SearchMan/search3/MemNodes", new ChildrenWatcher()
		//            {
		//
		//                public void nodeAdded(String node)
		//                {
		//                    System.out.println("nodeAdded:" + node + "--" + Thread.currentThread().toString());
		//                }
		//
		//                public void nodeRemoved(String node)
		//                {
		//                    System.out.println("nodeRemoved:" + node + "--" + Thread.currentThread().toString());
		//                }
		//            });
		//		
		//		client.watchLongValueNode("/SearchMan/search3", new LongValueWatcher(){
		//				public void valueChaned(long l)
		//				{
		//					System.out.println("valueChaned:"+l);
		//				}
		//			});
		//		
		//		client.watchStrValueNode("/SearchMan/search3", new  ZKClient.StringValueWatcher(){
		//
		//				public void valueChaned(String l)
		//				{
		//				    System.out.println("valueChaneds:"+l);
		//				}
		//			});
		//		client.watchMaster("/SearchMan/search3", "11",  new MasterWatcher()
		//						   {
		//				public void masterChangeTo(String serverName)
		//				{
		//				    System.out.println("master:"+serverName);
		//				}
		//
		//				public void exceptionCaught(Throwable t)
		//				{
		//					t.printStackTrace();
		//				}
		//			});
		//
		//        new Thread()
		//        {
		//            public void run()
		//            {
		//                try
		//                {
		//                    Thread.currentThread().sleep(1000);
		//                }
		//                catch (InterruptedException e)
		//                {
		//                }
		//                for (int i = 0; i < 10000; i++)
		//                    try
		//                    {
		//                        //  System.out.println("cheack create "+i);
		//                        // CLIENT.createIfNotExist("/SearchMan/search3/MemNodes/"+i );
		//                    }
		//                    catch (Exception e)
		//                    {
		//                        e.printStackTrace();
		//                    }
		//            }
		//        }.start();
		//
		//        new Thread()
		//        {
		//            public void run()
		//            {
		//                for (int i = 0; i < 10000; i++)
		//                    try
		//                    {
		//                        System.out.println("cheack create " + i);
		//                        CLIENT.forceDelete("/SearchMan/search3/MemNodes/" + i);
		//                    }
		//                    catch (Exception e)
		//                    {
		//                        e.printStackTrace();
		//                    }
		//            }
		//        }.start();

		//        String [] paths= "/sss/sfsfsd/dfsd".split("/" );
		//        for(String s:paths)
		//        {
		//            System.out.println(s);
		//            }
		//        MasterWatcher w = new MasterWatcher()
		//        {
		//
		//            public void masterChangeTo(String serverName)
		//            {
		//                //System.out.println("Master is:" + serverName);
		//            }
		//
		//            public void exceptionCaught(Throwable t)
		//            {
		//            }
		//        };
		//
		//        ZKClient.get().watchMaster("/test/tt", "localhost:8080", w);


			}


	public Stat setData(final String path, final byte data[]) throws KeeperException, InterruptedException{
		return zooKeeper.setData(path, data, -1);
	}


	public void getDataFromZooKeeper(String localPath, String ZkPath) throws IOException, KeeperException, InterruptedException {
		if(zooKeeper.exists(ZkPath.replaceAll("/+", "/"), false) == null)
			throw new RuntimeException("The specific path does not exist!");
		List<String> childrenList = getChildren(ZkPath.replaceAll("/+", "/"));
		if(childrenList.size() == 0)
			throw new RuntimeException("Node not found!");
		for(String child:childrenList) {
			List<String> tmpList = getChildren((ZkPath + "/" + child).replaceAll("/+", "/"));
			if(!tmpList.isEmpty()) {
				File folder = new File(localPath + "/" + child);
				if(!folder.isDirectory()) folder.mkdirs();
				getDataFromZooKeeper(localPath + "/" + child, (ZkPath + "/" + child).replaceAll("/+", "/"));
			} else {
				BufferedOutputStream writer = new BufferedOutputStream(
						new FileOutputStream(localPath + "/" + child));
				byte[] d = getData((ZkPath + "/" + child).replaceAll("/+", "/"));
				if (d != null)
					writer.write(d);
				writer.close();
			}
		}
	}

	public void uploadDataFromDir(String localPath, String ZkPath) throws KeeperException, InterruptedException {
		File dir = new File(localPath);
		if(!dir.exists())
			throw new RuntimeException("The specific path does not exist!");
		createIfNotExist(ZkPath.replaceAll("/+", "/"));
		if(dir.isFile()){
			try{
				String newPath = new String(localPath);
				BufferedInputStream reader = new BufferedInputStream(new FileInputStream(newPath));
				byte[] buffer = new byte[(int) dir.length()];
				reader.read(buffer,0,buffer.length);
				setData(ZkPath.replaceAll("/+", "/"), buffer);
				reader.close();
			}catch(Exception e){
				log.error("write data to ZK error?", e);
			}
//			throw new RuntimeException("The specific folder id empty!");
		}else{
			File[] indexes = dir.listFiles();
			for(File index : indexes){
				String nameString = index.getName();
				createIfNotExist((ZkPath + "/" + nameString).replaceAll("/+", "/"));
				if (index.isDirectory()){
					uploadDataFromDir(localPath + "/" + nameString, (ZkPath + "/" + nameString).replaceAll("/+", "/"));
				} else {
					try {
						String newPath = new String(localPath + "/" + nameString);
						BufferedInputStream reader = new BufferedInputStream(new FileInputStream(newPath));
						byte[] buffer = new byte[(int) index.length()];
						reader.read(buffer,0,buffer.length);
						setData((ZkPath + "/" + nameString).replaceAll("/+", "/"), buffer);
						reader.close();
					} catch (Exception e) {
						log.error("write data to ZK error?", e);
					}
				}
			}
		}
	}

	public String upsertNode(String path, byte[] data) throws KeeperException, InterruptedException {
		if(zooKeeper.exists(path, false) != null) {
			setData(path, data);
			return "updated";
		} else {
			//			createIfNotExist(path);
			create(path, data);
			return "inserted";
		}
	}
	
	public String getConnAddress(){
		return StringUtils.join(connectAddress, ",");
	}
	
}
