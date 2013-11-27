/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shanbo.feluca.node.http;

import org.jboss.netty.handler.codec.http.DefaultHttpResponse;

/**

 */
public interface Handler {

	/**
	 * with "/"
	 * @return
	 */
	 public String getPath();
	 
	 public void handle(NettyHttpRequest req, DefaultHttpResponse resp);
	 
}
