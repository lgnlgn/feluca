package org.shanbo.feluca.node.task;

import org.shanbo.feluca.node.job.TaskExecutor;
import org.shanbo.feluca.node.job.FelucaJob.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class TestLocalSleepTask extends TaskExecutor{

	
	
	private final static String SLEEP = "sleep";
	private final static String LOOP = "loop";
	
	private int sleepMs = 10000;
	private int loops = 2;
	public TestLocalSleepTask(JSONObject conf) {
		super(conf);
		sleepMs = conf.getJSONObject("param").getInteger(SLEEP);
	}

	
	@Override
	public JSONArray parseConfForJob(JSONObject param) {
		JSONArray subJobSteps = new JSONArray(1);//only 1 step 
		JSONArray concurrentLevel = new JSONArray(1);// needs only 1 thread 
		JSONObject conf = baseConfTemplate(true);
		conf.getJSONObject("param").put(SLEEP, "4000");
		conf.getJSONObject("param").put(LOOP, "3");
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
		ConcurrentExecutor.submit(new Runnable() {
			public void run() {
				state = JobState.RUNNING;
				for(int i = 0 ; i < loops; i++){
					if (state == JobState.STOPPING){
						break;
					}
					try {
						Thread.sleep(sleepMs);
						
					} catch (InterruptedException e) {
					}
				}
				if (state == JobState.STOPPING){
					state = JobState.INTERRUPTED;
				}else
					state = JobState.FINISHED;
			}
		});
	}

	@Override
	public void kill() {
		state = JobState.STOPPING;
	}

}
