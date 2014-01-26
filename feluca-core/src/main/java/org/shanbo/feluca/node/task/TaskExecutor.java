package org.shanbo.feluca.node.task;

import org.apache.commons.lang.math.RandomUtils;
import org.shanbo.feluca.node.job.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * execute task through running another process
 * @author lgn
 *
 */
public abstract class TaskExecutor {
	
	protected volatile JobState state;
	protected Logger log ;
	protected long taskID ;
	
	public abstract String getTaskFinalMessage();
	
	public TaskExecutor(JSONObject conf) {
		if (conf != null){
			log = LoggerFactory.getLogger(this.getClass());
			taskID = RandomUtils.nextLong();
			init(conf);
		}
	}
	
	
	protected abstract void init(JSONObject initConf);
	
	public abstract boolean isLocalJob();
	

	/**
	 * <li>invoke by FelucaJob</li>
	 * <li><b>consider it a static method! allow only 1 type : distrib or local</b></li>
	 * <li>create a list interpret the subjob's steps & concurrent-level</li>
	 * <li><b>remember: conf is for generating SubJob. A distributeJob must be parsed into 2 types : (DISTRIB for leader) && (LOCAL for worker)</b></li>
	 * <li>format: [[{type:local, <b>task:xxx</b>, param:{xxx}},{},{concurrent-level}],[]... [steps]]</li>
	 * @return
	 */
	public JSONArray arrangeSubJob(JSONObject global){
		if (!isLocalJob()){
			if ("local".equalsIgnoreCase(global.getString("type"))){ 
				return localTypeSubJob(global);
			}else{
				return distribTypeSubJob(global);
			}
		}else{
			return localTypeSubJob(global);
		}
	}
	
	/**
	 * <li>create a list interpret the subjob's steps & concurrent-level</li>
	 * @return
	 */
	protected abstract JSONArray localTypeSubJob(JSONObject global);
	
	/**
	 * <li>create a list interpret the subjob's steps & concurrent-level</li>
	 * @return
	 */
	protected abstract JSONArray distribTypeSubJob(JSONObject global);

	
	
	
	public abstract String getTaskName();
	
	public abstract void execute();
	
	public abstract void kill();
	
	public JobState currentState(){
		//get state through Process
		return  state;
	}
	
	protected JSONObject reformNewConf(boolean isExplicitLocal){
		JSONObject conf = new JSONObject();
		conf.put("param", new JSONObject());
		if (isLocalJob() || isExplicitLocal){
			conf.put("type", "local");
			conf.put("task",this.getClass().getName());
		}else{
			conf.put("type", "distrib");
			conf.put("task",this.getTaskName());
		}
		return conf;
	}
	
	
	
	
}
