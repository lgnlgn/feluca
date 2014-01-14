package org.shanbo.feluca.node.job;

import org.shanbo.feluca.node.job.FelucaJob.JobState;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class TaskExecutor {
	
	JobState state;
	
	public abstract JSONObject parseConfForTask();
	
	public abstract JSONArray parseConfForJob();
	
	public abstract String getTaskName();
	
	public abstract void execute();
	
	public abstract void kill();
	
	public JobState currentState(){
		return  state;
	}
	
}
