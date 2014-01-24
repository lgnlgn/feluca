package org.shanbo.feluca.node.task;

import java.util.concurrent.Future;

import org.shanbo.feluca.node.job.FelucaJob.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class LocalSleepTask extends TaskExecutor{
	public final static String SLEEP = "sleep";

	protected int sleepMs ;
	private Future<?> sleep;

	public LocalSleepTask(JSONObject conf) {
		super(conf);
	}

	protected void init(JSONObject conf){
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP) == null?10000: conf.getJSONObject("param").getInteger(SLEEP);
	}


	@Override
	public JSONArray arrangeSubJob(JSONObject global) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject conf = reformNewConf(true);
		conf.getJSONObject("param").put(SLEEP, "30000");
		JSONObject param  = global.getJSONObject("param");
		if (param != null)
			conf.getJSONObject("param").putAll(param); //using user-def's parameter
		
		concurrentLevel.add(conf);
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getTaskName() {
		return "lsleep";
	}

	@Override
	public void execute() {
		sleep = ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				state = JobState.RUNNING;
				System.out.println("----------run :" + taskID );
				System.out.println("sleep:" + sleepMs );

				//break this by cancel future; this is the only way,
				//otherwise you will have to wait until it's stop
				try {
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					state = JobState.STOPPING;
				}

				System.out.println("-----------awaking~~~~~~~~~~~~");
				if (state == JobState.STOPPING){
					state = JobState.INTERRUPTED;
				}else
					state = JobState.FINISHED;
				System.out.println("-----------awake~~~~~~~~ " + state);
			}
		});
	}

	@Override
	public void kill() {
		System.out.println("killing");
		sleep.cancel(true);
	}

	@Override
	public boolean isLocalJob() {
		return true;
	}

}
