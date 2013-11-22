package org.shanbo.feluca.node;

import org.shanbo.feluca.node.leader.LeaderServer;

/**
 * 
 *  @Description: TODO
 *	@author shanbo.liang
 */
public class FelucaMain {

	public static void main(String[] args) {
		//TODO
		LeaderServer server = new LeaderServer();
		server.start();
	}
}
