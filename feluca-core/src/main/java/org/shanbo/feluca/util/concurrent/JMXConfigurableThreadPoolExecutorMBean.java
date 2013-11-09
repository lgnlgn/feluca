package org.shanbo.feluca.util.concurrent;

public interface JMXConfigurableThreadPoolExecutorMBean
{
	/**
	 * Get the current number of running tasks
	 */
	public int getActiveCount();

	/**
	 * Get the number of completed tasks
	 */
	public long getCompletedTasks();

	/**
	 * Get the number of tasks waiting to be executed
	 */
	public long getPendingTasks();

	void setCorePoolSize(int n);
	void setMaximumPoolSize(int n);
	int getMaximumPoolSize();

	int getCorePoolSize();

}
