package org.shanbo.feluca.node.job.task;

import java.util.concurrent.Future;

import org.shanbo.feluca.node.job.JobState;
import org.shanbo.feluca.node.job.TaskExecutor;
import com.alibaba.fastjson.JSONObject;

/**
 * just for test
 * @author lgn
 *
 */
public class LocalSleepTask extends TaskExecutor{
	public final static String SLEEP = "sleep";

	String message = null;
	protected int sleepMs ;
	private Future<?> sleep;

	public LocalSleepTask(JSONObject conf) {
		super(conf);
	}

	protected void init(JSONObject conf){
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP) == null?10000: conf.getJSONObject("param").getInteger(SLEEP);
	}


	@Override
	public String getTaskName() {
		return "lsleep";
	}

	@Override
	protected void _exec() {

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
			message = "break";
			state = JobState.INTERRUPTED;
		}else{
			message = "finish";
			state = JobState.FINISHED;
		}
		System.out.println("-----------awake~~~~~~~~ " + state);
	}


	@Override
	public void kill() {
		System.out.println("killing");
		sleep.cancel(true);
	}


	@Override
	public String getTaskFinalMessage() {
		return message;
	}
}
