package org.shanbo.feluca.common;

import java.util.Properties;

import org.shanbo.feluca.common.FelucaJob.JobState;
import org.shanbo.feluca.util.concurrent.ConcurrentExecutor;

/**
 * task is a special kind of job, i.e. , leaf of the job tree;
 *  @Description TODO
 *	@author shanbo.liang
 */
public abstract class FelucaTask extends FelucaJob{

	public FelucaTask(Properties prop) {
		super(prop);
	}
	
	public abstract class StoppableTask implements Runnable{
		
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
	
	abstract protected StoppableTask createStoppableTask();
		
	public void stopJob(){
		this.state = JobState.STOPPING;
	}
	
	public void startJob(){
		state = JobState.RUNNING;
		ConcurrentExecutor.submit(createStoppableTask());
	}

}
