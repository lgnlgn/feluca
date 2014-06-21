package org.shanbo.feluca.node.task;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.shanbo.feluca.common.Constants;
import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class RuntimeTask extends TaskExecutor{

	//TODO 
	public static class StreamGobbler extends Thread {
	    InputStream is;
	    String      type;
	    OutputStream os;
	    StreamGobbler(InputStream is, String type) {
	        this(is, type, null);
	    }
	    StreamGobbler(InputStream is, String type, OutputStream redirect) {
	        this.is = is;
	        this.type = type;
	        this.os = redirect;
	    }
	    public void run() {
	        try {
	            PrintWriter pw = null;
	            if (os != null)
	                pw = new PrintWriter(os);
	            InputStreamReader isr = new InputStreamReader(is);
	            BufferedReader br = new BufferedReader(isr);
	            String line = null;
	            while ((line = br.readLine()) != null) {
	                if (pw != null)
	                    pw.println(line);
//	                System.out.println(type + ">" + line);
	            }
	            if (pw != null)
	                pw.flush();
	        } catch (IOException ioe) {
	            ioe.printStackTrace();
	        }
	    }
	}
	
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
		this.taskName =  initConf.getJSONObject("param").getString("name");
		String repo = initConf.getJSONObject("param").getString("repo");
		String resDir = repo + Constants.Base.RESOURCE_DIR;
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
				
				int exitValue = 1;
				try {
					exitValue = process.waitFor();
				} catch (InterruptedException e) {
					state = JobState.INTERRUPTED;
				}
				if (exitValue == 0){
					state = JobState.FINISHED;
				}else if (state == JobState.STOPPING){
					state = JobState.INTERRUPTED;
				}else{
					state = JobState.FAILED;
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
