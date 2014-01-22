package org.shanbo.feluca.node.task;

import java.util.concurrent.Future;

import org.shanbo.feluca.node.job.TaskExecutor;
import org.shanbo.feluca.node.job.FelucaJob.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class TestLocalSleepTask extends TaskExecutor{
	private final static String SLEEP = "sleep";

	private int sleepMs ;
	private Future<?> sleep;

	public TestLocalSleepTask(JSONObject conf) {
		super(conf);
	}

	protected void init(JSONObject conf){
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP) == null?10000: conf.getJSONObject("param").getInteger(SLEEP);
	}


	@Override
	public JSONArray parseConfForJob(JSONObject param) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject conf = baseConfTemplate(true);
		conf.getJSONObject("param").put(SLEEP, "30000");
		if (param != null)
			conf.getJSONObject("param").putAll(param); //using user-def's parameter
		conf.put("task", this.getClass().getName()); //do not forget
		concurrentLevel.add(conf);
		subJobSteps.add(concurrentLevel);
		return subJobSteps;
	}

	@Override
	public String getTaskName() {
		return "sleep";
	}

	@Override
	public void execute() {
		sleep = ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				state = JobState.RUNNING;
				System.out.println("----------run :" + taskID );
				System.out.println("sleep:" + sleepMs );

				try {
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					state = JobState.STOPPING;
				}

				System.out.println("-----------awake~~~~~~~~~~~~");
				if (state == JobState.STOPPING){
					state = JobState.INTERRUPTED;
				}else
					state = JobState.FINISHED;
			}
		});
	}

	@Override
	public void kill() {
		System.out.println("killing");
		sleep.cancel(true);
		state = JobState.STOPPING;
	}

	@Override
	protected boolean isLocalJob() {
		return true;
	}

}
