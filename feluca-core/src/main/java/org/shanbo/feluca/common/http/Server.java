package org.shanbo.feluca.common.http;


public interface Server {
	public void init();

	public void start();

	public void stop();

	public String serverName();
}
