package org.shanbo.feluca.node.job;

import java.util.HashMap;
import java.util.Map;

import org.shanbo.feluca.node.job.task.FileDeleteTask;
import org.shanbo.feluca.node.job.task.FilePullTask;
import org.shanbo.feluca.node.job.task.LocalSleepTask;
import org.shanbo.feluca.node.job.task.RuntimeTask;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class SubJobAllocator {
	private static Map<String, TaskExecutor> TASKS = new HashMap<String, TaskExecutor>();
	static{
		addTask(new LocalSleepTask(null));
		addTask(new FilePullTask(null));
		addTask(new FileDeleteTask(null));
		addTask(new RuntimeTask(null));
	}
	private static void addTask(TaskExecutor task){
		TASKS.put(task.getTaskName(), task);
	}
	
	public static TaskExecutor getTask(String taskName){
		return TASKS.get(taskName);
	}
	
	public static JSONObject getTaskTicket(String taskName){
		return getTask(taskName).taskSerialize(taskName);
	}
	

	/**
	 * implement function that meets taskSer decide 
	 * <li> if this is a localJob : simply put parameter into taskSerialize() result
	 * <li> if this is a distributeJob : you need:
	 *  <li><b> 1. check if this is a leader sent job which contains a 'type':"local" ;
	 *  <li><b> 2. else you have to decide what to send 
	 * @param properties
	 * @return
	 */
	public abstract JSONArray allocateSubJobs(JSONObject properties);
	
	public abstract String getName();
}
