package org.shanbo.feluca.node.job;

import com.alibaba.fastjson.JSONObject;

public abstract class TaskExecutor {
	public abstract JSONObject parseConfForTask();
	
	public abstract JSONObject parseConfForSubJob();
}
