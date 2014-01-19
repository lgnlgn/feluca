package org.shanbo.feluca.node.job;

import org.shanbo.feluca.node.job.FelucaJob.JobState;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * execute task through running another process
 * @author lgn
 *
 */
public abstract class TaskExecutor {
	
	private JobState state;
	
	public abstract JSONObject parseConfForTask();
	
	public abstract JSONArray parseConfForJob();
	
	public abstract String getTaskName();
	
	public abstract void execute();
	
	public abstract void kill();
	
	public JobState currentState(){
		//get state through Process
		return  state;
	}
	
}
