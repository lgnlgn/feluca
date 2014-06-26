package org.shanbo.feluca.node.job;

import java.util.Random;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * execute task through running another process
 * <p> basic task 
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
			taskID = new Random().nextLong();
			init(conf);
		}
	}
	
	
	protected abstract void init(JSONObject initConf);

	
	/**
	 * <li>invoke by FelucaJob</li>
	 * <li>this is just a delegator of xxxTypeSubJob</li>
	 * <li><b>consider it a static method! allow only 1 type : distrib or local</b></li>
	 * <li>create a list interpreting the subjob's steps & concurrent-level</li>
	 * <li><b>remember: conf is for generating SubJob. A distributeJob must be parsed into 2 types : (DISTRIB for leader) && (LOCAL for worker)</b></li>
	 * <li>format: [[{type:local, <b>task:xxx</b>, param:{xxx}},{},{concurrent-level}],[]... [steps]]</li>
	 * <p><i>You may want to override this , but most of time you don't need to </i>
	 * @return
	 */
//	public JSONArray arrangeSubJob(JSONObject global){
//		if (!isLocalJob()){
//			if ("local".equalsIgnoreCase(global.getString("type"))){ 
//				return localTypeSubJob(global);
//			}else{
//				return distribTypeSubJob(global);
//			}
//		}else{
//			return localTypeSubJob(global);
//		}
//	}
	
	/**
	 * <li>create a list interpret the subjob's steps & concurrent-level</li>
	 * @return
	 */
//	protected abstract JSONArray localTypeSubJob(JSONObject global);
	
	/**
	 * <li>create a list interpret the subjob's steps & concurrent-level</li>
	 * @return
	 */
//	protected abstract JSONArray distribTypeSubJob(JSONObject global);

	
	
	
	public abstract String getTaskName();
	
	public void execute(){
		ConcurrentExecutor.submit(new Runnable() {
			@Override
			public void run() {
				_exec();
			}
		});
	}
	
	protected abstract void _exec();
	
	public abstract void kill();
	
	public JobState currentState(){
		//get state through Process
		return  state;
	}
	
	
//	/**
//	 * default, to change to remote type, use {@link #taskSerialize(String)}
//	 * <p>almost all tasks don't need to override it
//	 * @param isExplicitLocal
//	 * @return
//	 */
//	public JSONObject taskSerialize(){
//		JSONObject conf = new JSONObject();
//		JSONObject para = new JSONObject();
//		para.put("repo", Constants.Base.getLeaderRepository());//default single machine job
//		conf.put("param", para);
//		conf.put("type", "local"); 
//		//change type to distribute by add a new 'address' to this conf
//		// conf.put("address", [...])
//		conf.put("task",this.getClass().getName());
//		return conf;
//	}
	
	/**
	 * remote task ticket
	 * @param distribTask
	 * @return
	 */
	public JSONObject taskSerialize(String distribTask){
		JSONObject conf = new JSONObject();
		JSONObject para = new JSONObject();
		para.put("repo", Constants.Base.getLeaderRepository());//default single machine job
		conf.put("param", para);
		//change type to distribute by add a new 'address' to this conf
		// conf.put("address", [...])
		conf.put("task", distribTask);
		return conf;
	}
	
	
}
