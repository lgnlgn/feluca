package org.shanbo.feluca.node;


public interface Server {
	public void init();

	public void start();

	public void stop();

	public String serverName();
}
