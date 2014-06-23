package org.shanbo.feluca.node.job.subjob;

import java.util.HashMap;
import java.util.Map;

import org.shanbo.feluca.node.job.TaskExecutor;
import org.shanbo.feluca.node.job.task.FilePullTask;
import org.shanbo.feluca.node.job.task.LocalSleepTask;
import org.shanbo.feluca.node.job.task.RuntimeTask;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class SubJobAllocator {
	protected static Map<String, TaskExecutor> TASKS = new HashMap<String, TaskExecutor>();
	static{
		addTask(new LocalSleepTask(null));
		addTask(new FilePullTask(null));
		addTask(new RuntimeTask(null));
	}
	private static void addTask(TaskExecutor task){
		TASKS.put(task.getTaskName(), task);
	}
	

	
	public abstract JSONArray allocateSubJobs(JSONObject properties);
	
	public abstract String getName();
}
