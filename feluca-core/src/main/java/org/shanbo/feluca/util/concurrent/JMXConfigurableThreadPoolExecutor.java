package org.shanbo.feluca.util.concurrent;

import java.lang.management.ManagementFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JMXConfigurableThreadPoolExecutor
	extends ThreadPoolExecutor
	implements JMXConfigurableThreadPoolExecutorMBean
{

	private final String mbeanName;
	
	public static JMXConfigurableThreadPoolExecutor newCachedThreadPool(String threadPoolName)
	{
	return new JMXConfigurableThreadPoolExecutor(0, Integer.MAX_VALUE,
								  60L, TimeUnit.SECONDS,
								  new SynchronousQueue<Runnable>(),
								   new NamedThreadFactory(threadPoolName));
	}

	public JMXConfigurableThreadPoolExecutor(String threadPoolName)
	{
		 
		this(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
	}
	public JMXConfigurableThreadPoolExecutor(int coreSize,  String threadPoolName)
	{
		 
		this(coreSize, coreSize <100? coreSize*2:coreSize, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
	}
	public JMXConfigurableThreadPoolExecutor(int coreSize,int maxsize, int queuelength, String threadPoolName)
	{
		 
		this(coreSize, maxsize, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(queuelength), new NamedThreadFactory(threadPoolName));
	}
	
	public JMXConfigurableThreadPoolExecutor(int coreSize, int maxsize,  String threadPoolName)
	{
		 
		this(coreSize,maxsize,30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
	}
	
	

	public JMXConfigurableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
											 BlockingQueue<Runnable> workQueue, NamedThreadFactory threadFactory)
	{
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
		super.allowCoreThreadTimeOut(true);

		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		mbeanName = "org.woyo.search.util.concurrent:type=" + threadFactory.getId();
		try
		{
			mbs.registerMBean(this, new ObjectName(mbeanName));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	private void unregisterMBean()
	{
		try
		{
			ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(mbeanName));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void shutdown()
	{
		// synchronized, because there is no way to access super.mainLock, which would be
		// the preferred way to make this threadsafe
		if (!isShutdown())
		{
			unregisterMBean();
		}
		super.shutdown();
	}

	@Override
	public synchronized List<Runnable> shutdownNow()
	{
		// synchronized, because there is no way to access super.mainLock, which would be
		// the preferred way to make this threadsafe
		if (!isShutdown())
		{
			unregisterMBean();
		}
		return super.shutdownNow();
	}

	/**
	 * Get the number of completed tasks
	 */
	public long getCompletedTasks()
	{
		return getCompletedTaskCount();
	}

	/**
	 * Get the number of tasks waiting to be executed
	 */
	public long getPendingTasks()
	{
		return getTaskCount() - getCompletedTaskCount();
	}
	public int getQueueSize()
	{
		return this.getQueue().size();
	}

	 
}
