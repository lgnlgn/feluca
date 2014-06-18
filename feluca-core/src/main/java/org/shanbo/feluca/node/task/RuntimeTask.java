package org.shanbo.feluca.node.task;

import java.util.ArrayList;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class RuntimeTask extends TaskExecutor{

	public final static String JARS = "jars";
	
	String taskName;
	Process process;
	StringBuilder message;
	ArrayList<String> args;
	String jars ;
	
	public RuntimeTask(JSONObject conf) {
		super(conf);
	
	}

	@Override
	public String getTaskFinalMessage() {
		return message.toString();
	}

	@Override
	protected void init(JSONObject initConf) {
		message = new StringBuilder();
		args = new ArrayList<String>();
		jars = initConf.getJSONObject("param").getString(JARS) == null?"": initConf.getJSONObject("param").getString(JARS);
	}

	@Override
	public boolean isLocalJob() {
		return true;
	}

	@Override
	protected JSONArray localTypeSubJob(JSONObject global) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected JSONArray distribTypeSubJob(JSONObject global) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTaskName() {
		return "runtime";
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void kill() {
		// TODO Auto-generated method stub
		
	}

}
