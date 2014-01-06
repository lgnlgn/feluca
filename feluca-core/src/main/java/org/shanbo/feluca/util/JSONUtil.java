package org.shanbo.feluca.util;

import com.alibaba.fastjson.JSONObject;

public class JSONUtil {
	public static String getJson(JSONObject json, String key, String defaultValue){
		String string = json.getString(key);
		if (string == null){
			return defaultValue;
		}return string;
	}
	
	public static int getJson(JSONObject json, String key, int defaultValue){
		Integer i = json.getInteger(key);
		if (i == null){
			return defaultValue;
		}
		return i;
	}

	public static double getJson(JSONObject json, String key, double defaultValue){
		Double i = json.getDouble(key);
		if (i == null){
			return defaultValue;
		}
		return i;
	}
	
}