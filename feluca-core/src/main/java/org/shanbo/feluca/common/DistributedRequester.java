package org.shanbo.feluca.common;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface DistributedRequester {
	
	/**
	 * you should implement it in a parallel style
	 * @param action
	 * @param audiences
	 * @return
	 */
	public List<String> request(String action, Object content, List<String> audiences) throws InterruptedException, ExecutionException;
}
