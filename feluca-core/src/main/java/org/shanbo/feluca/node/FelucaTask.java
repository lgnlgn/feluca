package org.shanbo.feluca.node;

import java.util.Properties;

import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

import com.alibaba.fastjson.JSONObject;

/**
 * task is a special kind of job, i.e. , leaf of the job tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaTask extends FelucaJob{
	
	public FelucaTask(JSONObject prop) {
		super(prop);
	}
	
	public abstract class StoppableRunning implements Runnable{
		
		protected abstract void runTask(); 
		
		protected abstract boolean isTaskSuccess();
		
		public void run(){
			runTask();
			if (isTaskSuccess()){
				state = JobState.FINISHED;
			}else if (state == JobState.STOPPING){
				state = JobState.INTERRUPTED;
			}else{
				state = JobState.FAILED;
			}
		}
	}
	
	abstract protected boolean canTaskRun();
	
	abstract protected StoppableRunning createStoppableTask();
		
	public void stopJob(){
		this.state = JobState.STOPPING;
	}
	
	public void startJob(){
		state = JobState.RUNNING;
		ConcurrentExecutor.submit(createStoppableTask());
	}

	
	public static class LeaderSupervisorTask extends FelucaTask{

		public LeaderSupervisorTask(JSONObject prop) {
			super(prop);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected boolean canTaskRun() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		protected StoppableRunning createStoppableTask() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected String getAllLog() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
}
