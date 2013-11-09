package org.shanbo.feluca.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory
{
    protected final String id;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id)
    {
        this.id = id;
    }
	

    public Thread newThread(Runnable runnable)
    {        
        String name = id + ":" + n.getAndIncrement();
        return new Thread(runnable, name);
    }

	public String getId()
	{
		return id;
	}
}
