package org.shanbo.feluca.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.shanbo.feluca.node.http.HttpRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DistributedRequester {
	static Logger log = LoggerFactory.getLogger(Config.class);
	
	private static final DistributedRequester instance = new HttpRequester();
	
	public abstract List<String> broadcast(String action, Object content, List<String> audiences) throws InterruptedException, ExecutionException ;

	public abstract List<String> request(List<Callable<String>> callables) throws InterruptedException, ExecutionException ;
	
	
	public static DistributedRequester get(){
		return instance;
	}
}
