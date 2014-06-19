package org.shanbo.feluca.node.task;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class RuntimeTask extends TaskExecutor{

	public final static String CLASSPATH = "-cp";
	public final static String ARGUMENTS = "-arg";
	
	String taskName;
	Process process;
	StringBuilder message;
	String[] cmd;

	
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

		List<String> cmds = new ArrayList<String>();
		String newcp = initConf.getJSONObject("param").getString(CLASSPATH) == null?"": initConf.getJSONObject("param").getString(CLASSPATH);
		String args = initConf.getJSONObject("param").getString(ARGUMENTS) == null?"": initConf.getJSONObject("param").getString(ARGUMENTS);
		taskName =  initConf.getJSONObject("param").getString("name");
		String resDir = Constants.Base.getWorkerRepository() + Constants.Base.RESOURCE_DIR;
		String cp = System.getProperty("java.class.path");
		for(String jar : newcp.split(",|;|:")){
			cp += System.getProperty("path.separator") + resDir + "/" + jar;
		}
		cp +=   System.getProperty("path.separator") + resDir;
		cmds.add("java");
		cmds.add("-cp");
		cmds.add(cp);
		for(String arg : args.split("\\s+")){
			cmds.add(arg);
		}
		cmd = new String[cmds.size()];
		cmds.toArray(cmd);
	}

	@Override
	public boolean isLocalJob() {
		return true;
	}

	@Override
	protected JSONArray localTypeSubJob(JSONObject global) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject conf = reformNewConf(true);
		JSONObject param  = global.getJSONObject("param");
		if (param != null)
			conf.getJSONObject("param").putAll(param); //using user-def's parameter
		
		concurrentLevel.add(conf);
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	protected JSONArray distribTypeSubJob(JSONObject global) {
		return null;
	}

	@Override
	public String getTaskName() {
		return "runtime";
	}

	@Override
	public void execute() {
		ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				state = JobState.RUNNING;
				try {
					process = Runtime.getRuntime().exec(cmd);
				} catch (IOException e) {
					state = JobState.FAILED;
				}
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = null;
				boolean finished = false;
				try {
					while((line = reader.readLine())!= null ){
						if (line.equals("Task finished!!")){
							finished = true;
						}
						message.append(line + "\n");
					}
				} catch (IOException e) {
					state = JobState.INTERRUPTED;
				}
				if (finished == true){
					state = JobState.FINISHED;
				}else {
					state = JobState.INTERRUPTED;
				}
			}
		});
	}

	@Override
	public void kill() {
		state = JobState.STOPPING;
		process.destroy();
	}

}
