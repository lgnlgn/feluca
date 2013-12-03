package org.shanbo.feluca.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public abstract class LogStorage {
	
	public abstract void storeJobLogs(String jobName, JSONObject JobInfo);
	
	public abstract JSONObject searchJobInfo(String jobName);
	
	public abstract JSONArray searchJobName(long msDateTimeStart, long msDateTimeEnd);
	
}
