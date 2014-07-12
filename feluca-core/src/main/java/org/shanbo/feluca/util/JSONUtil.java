package org.shanbo.feluca.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;

import com.alibaba.fastjson.JSONArray;
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
	
	public static JSONArray fromStrings(Object... array){
		JSONArray ja = new JSONArray();
		for(Object e : array){
			ja.add(e);
		}
		return ja;
	}
	
	public static JSONObject fromStrPairs(Pair...  pairs){
		JSONObject jo = new JSONObject();
		for(Pair p : pairs){
			jo.put(p.getKey().toString(), p.getValue());
		}
		return jo;
	}
	
	public static JSONArray listToAJsonArray(Collection list){
		return new JSONArray(new ArrayList(list));
	}
	
	public static List<String> JSONArrayToList(JSONArray ja){
		if (ja == null){
			return Collections.emptyList();
		}
		List<String> result = new ArrayList<String>();
		for(Object obj : ja){
			result.add((obj instanceof String )?(String)obj : obj .toString());
		}
		return result;
	}
	
	public static void main(String[] args) {
		JSONArray ja = new JSONArray();
		ja.add("aaa");
		ja.add("bbb");
		JSONObject qJsonObject = new JSONObject();
		System.out.println(qJsonObject.getJSONArray("a"));
	}
	
	public static JSONObject fromProperties(Properties p){
		JSONObject json = new JSONObject();
		for(Object key : p.keySet()){
			json.put(key.toString(), p.getProperty(key.toString()));
		}
		return json;
	}
	
	public static JSONObject basicAlgoConf(int loops){
		JSONObject json = new JSONObject();
		json.put("loops", loops);
		return json;
	}
}
