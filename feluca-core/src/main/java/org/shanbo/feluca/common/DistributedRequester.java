package org.shanbo.feluca.common;

import java.util.List;

public interface DistributedRequester {
	
	/**
	 * you should implement it in a parallel style
	 * @param action
	 * @param audiences
	 * @return
	 */
	public List<String> request(Object action, List<String> audiences);
}
