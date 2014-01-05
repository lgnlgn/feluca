package org.shanbo.feluca.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.shanbo.feluca.node.job.FelucaJob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class LogStorage {
	
	private static LogStorage storage = new MemoryStorage(); 
	
	public abstract void storeJobLogs(JSONObject JobInfo);
	
	public abstract JSONObject searchJobInfo(String jobName);
	
	public abstract List<String> searchJobNames(long msDateTimeStart, long msDateTimeEnd);
	
	public abstract JSONArray getLastJobInfos(int size);
	
	public static LogStorage get(){
		return storage;
	}
	
	public static class MemoryStorage extends LogStorage{

		BlockingDeque<JSONObject> bag = new LinkedBlockingDeque<JSONObject>();
		
		final int maxJobSize = 10;
		
		@Override
		public void storeJobLogs(JSONObject JobInfo) {
			if (bag.size() >= maxJobSize){
				bag.pollFirst();
			}
			bag.addLast(JobInfo);
		}

		@Override
		public JSONObject searchJobInfo(String jobName) {
			if (jobName == null)
				return null;
			for(JSONObject jobInfo : bag){
				String jn = jobInfo.getString(FelucaJob.JOB_NAME);
				if (jobName.equals(jn)){
					return jobInfo;
				}
			}
			return null;
		}

		@Override
		public List<String> searchJobNames(long msDateTimeStart, long msDateTimeEnd) {
			List<String> result = new ArrayList<String>();
			if (msDateTimeStart >= msDateTimeEnd){
				return result;
			}
			for(JSONObject jobInfo : bag){
				long jobStart = jobInfo.getLongValue(FelucaJob.JOB_START_TIME);
				if (jobStart >= msDateTimeStart && jobStart <= msDateTimeEnd){
					result.add(jobInfo.getString(FelucaJob.JOB_NAME));
				}
			}
			return result;
		}

		@Override
		public JSONArray getLastJobInfos(int size) {
			JSONArray ja = new JSONArray();
			Iterator<JSONObject> dit = bag.descendingIterator();
			for(int i = 0 ; i < size && dit.hasNext(); i++){
				JSONObject job = dit.next();
				ja.add(JSONObject.parse(job.toJSONString()));
			}
			return ja;
		}
		
	}
	
}
