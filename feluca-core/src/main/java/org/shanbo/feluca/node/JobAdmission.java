package org.shanbo.feluca.node;

import com.alibaba.fastjson.JSONObject;

public class JobAdmission {
	private JSONObject global;
	private JSONObject distrib;
	
	private boolean legal = false;
	
	public JobAdmission(String text) {
		try{
			JSONObject conf = JSONObject.parseObject(text);
			global = conf.getJSONObject("global");
			distrib = conf.getJSONObject("distrib");
			legal = true;
		}catch(Exception e){
			;
		}
	}
	
}
