package org.shanbo.feluca.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.shanbo.feluca.node.job.FelucaJob;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class LogStorage {
	
	private static LogStorage storage = new MemoryStorage(); 
	
	public abstract void storeJobLogs(JSONObject JobInfo);
	
	public abstract JSONObject searchJobInfo(String jobName);
	
	public abstract List<JSONObject> searchJobs(long sysTimeStart, long sysTimeEnd);
	
	public abstract JSONArray getLastJobInfos(int size);
	
	public static LogStorage get(){
		return storage;
	}
	
	public static class MemoryStorage extends LogStorage{
		
		private static class JobInfoLastestSorter implements Comparator<JSONObject>{

			public int compare(JSONObject o1, JSONObject o2) {
				long t1 = o1.getLongValue(FelucaJob.JOB_START_TIME);
				long t2 = o2.getLongValue(FelucaJob.JOB_START_TIME);
				return t1>t2?-1:(t1==t2?0:1);
			}
			
		}
		
		Map<String, JSONObject> map = new ConcurrentHashMap<String, JSONObject>();
		final int maxJobSize = 10;
		
		private void removeEarliest(){
			long earliestStart = Long.MAX_VALUE;
			String earliestName = null;
			for(JSONObject jobInfo : map.values()){
				if (jobInfo.getLongValue(FelucaJob.JOB_START_TIME) < earliestStart){
					earliestStart = jobInfo.getLongValue(FelucaJob.JOB_START_TIME);
					earliestName = jobInfo.getString(FelucaJob.JOB_NAME);
				}
			}
			map.remove(earliestName);
		}
		
		@Override
		public void storeJobLogs(JSONObject JobInfo) {
			map.put(JobInfo.getString(FelucaJob.JOB_NAME), JobInfo);
			if (map.size() > maxJobSize){
				removeEarliest();
			}
		}

		@Override
		public JSONObject searchJobInfo(String jobName) {
			if (jobName == null)
				return null;
			return map.get(jobName);
		}

		@Override
		public List<JSONObject> searchJobs(long sysTimeStart, long sysTimeEnd) {
			List<JSONObject> result = new ArrayList<JSONObject>();
			if (sysTimeStart >= sysTimeEnd){
				return result;
			}
			for(JSONObject jobInfo : map.values()){
				long jobStart = jobInfo.getLongValue(FelucaJob.JOB_START_TIME);
				if (jobStart >= sysTimeStart && jobStart <= sysTimeEnd){
					result.add(jobInfo);
				}
			}
			Collections.sort(result, new JobInfoLastestSorter());
			return result;
		}

		@Override
		public JSONArray getLastJobInfos(int size) {
			List<JSONObject> result = new ArrayList<JSONObject>();
			result.addAll(map.values());
			Collections.sort(result, new JobInfoLastestSorter());
			JSONArray t = new JSONArray();
			t.addAll(result.subList(0, Math.min(result.size(), size)));
			return t;
		}
		
	}
	
}
